import abc
import datetime
import functools
import logging
import threading

import pendulum
import celery
import requests

from django.apps import apps
from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.db import transaction
from django.urls import reverse
from django.db.models.expressions import RawSQL

from share.change import ChangeGraph
from share.models import HarvestLog
from share.models import Source
from share.models import RawDatum, NormalizedData, ChangeSet, CeleryTask, CeleryProviderTask, ShareUser, SourceConfig


logger = logging.getLogger(__name__)


def getter(self, attr):
    return getattr(self.context, attr)


def setter(self, value, attr):
    return setattr(self.context, attr, value)


class LoggedTask(celery.Task):
    abstract = True
    CELERY_TASK = CeleryProviderTask

    # NOTE: Celery tasks are singletons.
    # If ever running in threads/greenlets/epoll self.<var> will clobber eachother
    # this binds all the attributes that would used to a threading local object to correct that
    # Python 3.x thread locals will apparently garbage collect.
    context = threading.local()
    # Any attribute, even from subclasses, must be defined here.
    THREAD_SAFE_ATTRS = ('args', 'kwargs', 'started_by', 'source', 'task', 'normalized', 'config')
    for attr in THREAD_SAFE_ATTRS:
        locals()[attr] = property(functools.partial(getter, attr=attr)).setter(functools.partial(setter, attr=attr))

    def run(self, started_by_id, *args, **kwargs):
        # Clean up first just in case the task before crashed
        for attr in self.THREAD_SAFE_ATTRS:
            if hasattr(self.context, attr):
                delattr(self.context, attr)

        self.args = args
        self.kwargs = kwargs
        self.started_by = ShareUser.objects.get(id=started_by_id)
        self.source = None

        self.setup(*self.args, **self.kwargs)

        assert issubclass(self.CELERY_TASK, CeleryTask)
        # TODO optimize into 1 query with ON CONFLICT
        self.task, _ = self.CELERY_TASK.objects.update_or_create(
            uuid=self.request.id,
            defaults=self.log_values(),
        )

        self.do_run(*self.args, **self.kwargs)

        # Clean up at the end to avoid keeping anything in memory
        # This is not in a finally as it may mess up sentry's exception reporting
        for attr in self.THREAD_SAFE_ATTRS:
            if hasattr(self.context, attr):
                delattr(self.context, attr)

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        CeleryTask.objects.filter(uuid=task_id).update(status=CeleryTask.STATUS.retried)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        CeleryTask.objects.filter(uuid=task_id).update(status=CeleryTask.STATUS.failed)

    def on_success(self, retval, task_id, args, kwargs):
        CeleryTask.objects.filter(uuid=task_id).update(status=CeleryTask.STATUS.succeeded)

    def log_values(self):
        return {
            'name': self.name,
            'args': self.args,
            'kwargs': self.kwargs,
            'started_by': self.started_by,
            'provider': self.source,
            'status': CeleryTask.STATUS.started,
        }

    @abc.abstractmethod
    def setup(self, *args, **kwargs):
        raise NotImplementedError()

    @abc.abstractmethod
    def do_run(self):
        raise NotImplementedError()


class AppTask(LoggedTask):

    def setup(self, app_label, *args, **kwargs):
        self.config = apps.get_app_config(app_label)
        self.source = self.config.user
        self.args = args
        self.kwargs = kwargs

    def log_values(self):
        return {
            **super().log_values(),
            'app_label': self.config.label,
            'app_version': self.config.version,
        }


# Backward-compatible hack until Tamandua's all set
class SourceTask(LoggedTask):

    def setup(self, app_label, *args, **kwargs):
        self.config = SourceConfig.objects.select_related('harvester', 'transformer', 'source__user').get(label=app_label)
        self.source = self.config.source.user
        self.args = args
        self.kwargs = kwargs


class HarvesterTask(SourceTask):

    @classmethod
    def resolve_date_range(cls, start, end):
        logger.debug('Coercing start and end (%r, %r) into UTC dates', start, end)

        if bool(start) ^ bool(end):
            raise ValueError('"start" and "end" must either both be supplied or omitted')

        if not start and not end:
            start, end = datetime.date.today() - datetime.timedelta(days=1), datetime.date.today()
        if type(end) is str:
            end = pendulum.timezone('utc').convert(pendulum.parse(end.rstrip('Z'))).date()
        if type(start) is str:
            start = pendulum.timezone('utc').convert(pendulum.parse(start.rstrip('Z'))).date()

        logger.debug('Interpretting start and end as %r and %r', start, end)
        return start, end

    @classmethod
    def _preapply(cls, targs=None, tkwargs=None, restarted=False, **kwargs):
        tkwargs = tkwargs or {}
        start, end = cls.resolve_date_range(tkwargs.get('start'), tkwargs.get('end'))

        # TODO This should not be here but it's the best place too hook in right now
        config = SourceConfig.objects.select_related('harvester', 'transformer').filter(label=targs[1])[0]
        log, created = HarvestLog.objects.get_or_create(
            end_date=end,
            start_date=start,
            source_config=config,
            harvester_version=config.harvester.version,
            source_config_version=config.version,
            defaults={'status': HarvestLog.STATUS.created}
        )

        if not created:
            if log.task_id:
                kwargs['task_id'] = str(log.task_id)  # Preserve the old task id
            log.share_version = settings.VERSION  # Update version, it may have changed

            if log.status != log.STATUS.rescheduled:
                log.status = log.STATUS.retried

            log.save()

        tkwargs['end'] = end.isoformat()
        tkwargs['start'] = start.isoformat()

        return targs, tkwargs, kwargs

    def apply(self, targs=None, tkwargs=None, restarted=False, **kwargs):
        targs, tkwargs, kwargs = self._preapply(targs, tkwargs, restarted=restarted, **kwargs)
        return SourceTask.apply(self, targs, tkwargs, **kwargs)

    def apply_async(self, targs=None, tkwargs=None, restarted=False, **kwargs):
        targs, tkwargs, kwargs = self._preapply(targs, tkwargs, restarted=restarted, **kwargs)
        return SourceTask.apply_async(self, targs, tkwargs, **kwargs)

    def log_values(self):
        return {
            **super().log_values(),
            'app_label': self.config.label,
            'app_version': self.config.harvester.version,
        }

    # start and end *should* be dates. They will be turned into dates if not
    def do_run(self, start=None, end=None, limit=None, force=False, superfluous=False, ignore_disabled=False, ingest=True, **kwargs):
        # WARNING: Errors that occur here cannot be logged to the HarvestLog.
        logger.debug('Loading harvester for %r', self.config)
        harvester = self.config.get_harvester()
        start, end = self.resolve_date_range(start, end)

        # TODO optimize into 1 query with ON CONFLICT
        log, created = HarvestLog.objects.get_or_create(
            end_date=end,
            start_date=start,
            source_config=self.config,
            harvester_version=self.config.harvester.version,
            source_config_version=self.config.version,
            defaults={'task_id': self.request.id}
        )

        # TODO search for logs that contain our date range.
        if not created and log.completions > 0:
            if not superfluous:
                log.skip(HarvestLog.SkipReasons.duplicated)
                return logger.warning('%s - %s has already been harvested for %r. Force a re-run with superfluous=True', start, end, self.config)
            else:
                logger.info('%s - %s has already been harvested for %r. Re-running superfluously', start, end, self.config)
        elif not created:
            log.task_id = self.request.id

        datum_ids = {True: [], False: []}
        for datum in harvester.harvest_from_log(log, force=force, ignore_disabled=ignore_disabled, limit=limit, **kwargs):
            datum_ids[datum.created].append(datum.id)
            if datum.created:
                logger.debug('Found new %r from %r', datum, self.config)
            else:
                logger.debug('Rediscovered new %r from %r', datum, self.config)

        logger.info('Collected %d new RawData from %r', len(datum_ids[True]), self.config)
        logger.debug('Rediscovered %d RawData from %r', len(datum_ids[False]), self.config)

        if not ingest:
            logger.warning('Not starting normalizer tasks, ingest = False')
        else:
            for raw_id in datum_ids[True]:
                task = NormalizerTask().apply_async((self.started_by.id, self.config.label, raw_id,))
                logger.debug('Started normalizer task %s for %s', task, raw_id)
            if superfluous:
                for raw_id in datum_ids[False]:
                    task = NormalizerTask().apply_async((self.started_by.id, self.config.label, raw_id,))
                    logger.debug('Superfluously started normalizer task %s for %s', task, raw_id)


class NormalizerTask(SourceTask):

    def do_run(self, raw_id):
        raw = RawDatum.objects.get(pk=raw_id)
        transformer = self.config.get_transformer()

        assert raw.suid.source_config_id == self.config.id, '{!r} is from {!r}. Tried parsing it as {!r}'.format(raw, raw.suid.source_config_id, self.config.id)

        logger.info('Starting normalization for %s by %s', raw, transformer)

        try:
            graph = transformer.transform(raw)

            if not graph or not graph['@graph']:
                logger.warning('Graph was empty for %s, skipping...', raw)
                return

            normalized_data_url = settings.SHARE_API_URL[0:-1] + reverse('api:normalizeddata-list')
            resp = requests.post(normalized_data_url, json={
                'data': {
                    'type': 'NormalizedData',
                    'attributes': {
                        'data': graph,
                        'raw': {'type': 'RawData', 'id': raw_id},
                        'tasks': [self.task.id]
                    }
                }
            }, headers={'Authorization': self.source.authorization(), 'Content-Type': 'application/vnd.api+json'})
        except Exception as e:
            logger.exception('Failed normalizer task (%s, %d)', self.config.label, raw_id)
            raise self.retry(countdown=10, exc=e)

        if (resp.status_code // 100) != 2:
            raise self.retry(countdown=10, exc=Exception('Unable to submit change graph. Received {!r}, {}'.format(resp, resp.content)))

        logger.info('Successfully submitted change for %s', raw)

    def log_values(self):
        return {
            **super().log_values(),
            'app_label': self.config.label,
            'app_version': self.config.transformer.version,
        }


class DisambiguatorTask(LoggedTask):

    def setup(self, normalized_id, *args, **kwargs):
        self.normalized = NormalizedData.objects.get(pk=normalized_id)
        self.source = self.normalized.source

    def do_run(self, *args, **kwargs):
        # Load all relevant ContentTypes in a single query
        ContentType.objects.get_for_models(*apps.get_models('share'), for_concrete_models=False)

        logger.info('%s started disambiguation for NormalizedData %s at %s', self.started_by, self.normalized.id, datetime.datetime.utcnow().isoformat())

        try:
            with transaction.atomic():
                cg = ChangeGraph(self.normalized.data['@graph'], namespace=self.normalized.source.username)
                cg.process()
                cs = ChangeSet.objects.from_graph(cg, self.normalized.id)
                if cs and (self.source.is_robot or self.source.is_trusted or Source.objects.filter(user=self.source).exists()):
                    # TODO: verify change set is not overwriting user created object
                    cs.accept()
        except Exception as e:
            logger.exception('Failed disambiguation for NormalizedData %s. Retrying...', self.normalized.id)
            raise self.retry(countdown=10, exc=e)

        logger.info('Finished disambiguation for NormalizedData %s by %s at %s', self.normalized.id, self.started_by, datetime.datetime.utcnow().isoformat())


class BotTask(AppTask):

    def do_run(self, last_run=None, **kwargs):
        bot = self.config.get_bot(self.started_by, last_run=last_run, **kwargs)
        logger.info('Running bot %s. Started by %s', bot, self.started_by)
        bot.run()


@celery.shared_task(bind=True)
def harvest_task(self, ingest=True, exhaust=True):
    with transaction.atomic(using='locking'):
        for _ in range(10):
            log = HarvestLog.objects.annotate(
                lock_acquired=RawSQL(
                    'pg_try_advisory_xact_lock(%s::regclass::integer, {}.{})'.format(
                        HarvestLog._meta.db_table,
                        HarvestLog._meta.get_field('source_config').column,
                    ), (SourceConfig._meta.db_table, )
                ),
                locked=RawSQL(
                    "EXISTS(SELECT * FROM pg_locks WHERE locktype = 'advisory' AND objid = share_sourceconfig.id AND classid = 'share_sourceconfig' :: REGCLASS :: INTEGER AND locktype = 'advisory')",
                    []
                )
            ).select_related('source_config').filter(
                locked=False,
                source_config__disabled=False,
                source_config__source__is_deleted=False,
                status__in=[
                    HarvestLog.STATUS.created,
                    HarvestLog.STATUS.started,
                    HarvestLog.STATUS.rescheduled,
                    HarvestLog.STATUS.cancelled,
                ]
            ).using('locking').first()

            # Sometimes another process will beat us to the lock
            # there doesn't appear to be a great way to avoid this
            # while only locking one row, especially when limitted to
            # Django's interface
            if log is None or log.lock_acquired:
                break

        if log is None:
            return logger.warning('No HarvestLogs are currently available')
        elif exhaust:
            logger.debug('Spawning another harvest task')
            harvest_task.apply_async((), {'ingest': ingest, 'exhaust': exhaust})

        # No need to lock, we've already acquired it here
        for datum in log.source_config.get_harvester().harvest_from_log(log, lock=False):
            if ingest and datum.created:
                NormalizerTask().apply_async((1, log.source_config.label, datum.id,))
