import logging

import celery
import pendulum
import requests

from django.apps import apps
from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.db import transaction
from django.db import models
from django.urls import reverse

from share.change import ChangeGraph
from share.models import ChangeSet
from share.models import HarvestLog
from share.models import NormalizedData
from share.models import RawDatum
from share.models import Source
from share.models import SourceConfig


logger = logging.getLogger(__name__)


@celery.shared_task(bind=True)
def transform(self, raw_id):
    raw = RawDatum.objects.get(pk=raw_id)
    transformer = raw.suid.source_config.get_transformer()

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
                    'tasks': [self.request.id]
                }
            }
        }, headers={'Authorization': self.source.authorization(), 'Content-Type': 'application/vnd.api+json'})
    except Exception as e:
        logger.exception('Failed normalizer task (%s, %d)', self.config.label, raw_id)
        raise self.retry(countdown=10, exc=e)

    if (resp.status_code // 100) != 2:
        raise self.retry(countdown=10, exc=Exception('Unable to submit change graph. Received {!r}, {}'.format(resp, resp.content)))

    logger.info('Successfully submitted change for %s', raw)


@celery.shared_task(bind=True, retries=5)
def disambiguate(self, normalized_id):
    normalized = NormalizedData.objects.get(pk=normalized_id)
    # Load all relevant ContentTypes in a single query
    ContentType.objects.get_for_models(*apps.get_models('share'), for_concrete_models=False)

    with transaction.atomic():
        cg = ChangeGraph(normalized.data['@graph'], namespace=normalized.source.username)
        cg.process()
        cs = ChangeSet.objects.from_graph(cg, normalized.id)
        if cs and (normalized.source.is_robot or normalized.source.is_trusted or Source.objects.filter(user=normalized.source).exists()):
            # TODO: verify change set is not overwriting user created object
            cs.accept()


@celery.shared_task(bind=True, retries=5)
def harvest(self, log_id=None, ignore_disabled=False, ingest=True, exhaust=True, superfluous=False, force=False):
    """Complete the harvest of the given HarvestLog or next the next available HarvestLog.

    Args:
        log_id (int, optional): Harvest the given log. Defaults to None.
            If the given log cannot be locked, the task will retry indefinitely.
            If the given log belongs to a disabled or deleted Source or SourceConfig, the task will fail.
        ingest (bool, optional): Whether or not to start the full ingest process for harvested data. Defaults to True.
        exhaust (bool, optional): Whether or not to start another harvest task if one is found. Defaults to True.
            Used to prevent a backlog of harvests. If we have a valid job, spin off another task to eat through
            the rest of the queue.
        superfluous (bool, optional): Re-ingest Rawdata that we've already collected. Defaults to False.

    """
    with transaction.atomic(using='locking'):
        qs = HarvestLog.objects.using('locking')

        if log_id is not None:
            logger.debug('Loading harvest log %d', log_id)
            qs = qs.filter(id=log_id)
        else:
            logger.debug('log_id was not specified, searching for an available log.')

            if ignore_disabled:
                qs = qs.exclude(
                    source_config__disabled=True,
                    source_config__source__is_deleted=True
                )

            qs = qs.filter(
                status__in=HarvestLog.READY_STATUSES
            ).unlocked('source_config')

        log = qs.acquire_lock('source_config').first()

        if log is None and log_id is None:
            return logger.warning('No HarvestLogs are currently available')
        elif log_id is not None:
            # If an id was given to us, we should have gotten a log
            log = HarvestLog.objects.get(id=log_id)  # Force the failure
            raise Exception('Failed to load {} but then found {!r}.'.format(log_id, log))

        # Additional attributes for the celery backend
        # Allows for better analytics of currently running tasks
        self.update_state(meta={
            'log_id': log.id,
            'source': log.source_config.source.long_title,
            'source_config': log.source_config.label,
        })

        if log_id and not log.lock_acquired and not force:
            logger.warning('Could not lock the specified log, %r. Retrying in %d seconds.', log)
            raise self.retry()  # TODO
        elif not log.lock_acquired and not force:
            return logger.info('Could not lock any available harvest logs.')
        elif not log.lock_acquired and force:
            logger.warning('Could not acquire lock on %r. Proceeeding anyways.', log)

        if exhaust and log_id is None:
            if force:
                logger.warning('propagating force=True until queue exhaustion')

            logger.debug('Spawning another harvest task')
            res = harvest.apply_async(self.request.args, self.request.kwargs)
            logger.info('Spawned %r', res)

        logger.info('Harvesting %r', log)

        # No need to lock, we've already acquired it here
        for datum in log.source_config.get_harvester().harvest_from_log(log, lock=False, force=force, ignore_disabled=ignore_disabled):
            if ingest and (datum.created or superfluous):
                transform.apply_async((datum.id, ))


@celery.shared_task(bind=True)
def schedule_harvests(self, *source_config_ids, cutoff=None):
    """

    Args:
        *source_config_ids (int): PKs of the source configs to schedule harvests for.
            If omitted, all non-disabled and non-deleted source configs will be scheduled
        cutoff (optional, datetime): The time to schedule harvests up to. Defaults to today.

    """
    if cutoff is None:
        cutoff = pendulum.utcnow().date()

    if source_config_ids:
        qs = SourceConfig.objects.filter(id__in=source_config_ids)
    else:
        qs = SourceConfig.objects.exclude(disabled=True).exclude(source__is_deleted=True)

    # TODO This could be much more efficient
    with transaction.atomic():
        logs = []

        for source_config in qs.select_related('harvester').annotate(latest=models.Max('harvest_logs__end_date')):
            if not source_config.latest and source_config.backharvesting and source_config.earliest_date:
                end_date = source_config.earliest_date
            elif source_config.latest is None:
                end_date = cutoff - source_config.harvest_interval
            else:
                end_date = source_config.latest

            log_kwargs = {
                'source_config': source_config,
                'source_config_version': source_config.version,
                'harvester_version': source_config.get_harvester().VERSION,
            }

            while end_date + source_config.harvest_interval <= cutoff:
                start_date = end_date
                end_date = end_date + source_config.harvest_interval

                logs.append(HarvestLog(start_date=start_date, end_date=end_date, **log_kwargs))

        HarvestLog.objects.bulk_create(logs)
