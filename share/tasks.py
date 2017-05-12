import logging
import datetime

import celery
import requests

from django.apps import apps
from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.db import transaction
from django.urls import reverse

from share.change import ChangeGraph
from share.models import ChangeSet
from share.models import HarvestLog
from share.models import NormalizedData
from share.models import RawDatum
from share.models import Source
from share.models import SourceConfig
from share.celery import CeleryTask


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


@celery.shared_task(bind=True, base=CeleryTask)
def harvest(self, ingest=True, exhaust=True, superfluous=False):
    """

    Args:
        ingest (bool, optional): Whether or not to start the full ingest process for harvested data. Defaults to True.
        exhaust (bool, optional): Whether or not to start another harvest task if one is found. Defaults to True.
            Used to prevent a backlog of harvests. If we have a valid job, spin off another task to eat through
            the rest of the queue.
        superfluous (bool, optional): Re-ingest Rawdata that we've already collected. Defaults to False.
    """
    with transaction.atomic(using='locking'):
        log = HarvestLog.objects.lock_next_available()

        if log is None:
            return logger.warning('No HarvestLogs are currently available')
        elif exhaust:
            logger.debug('Spawning another harvest task')
            harvest.apply_async((), {'ingest': ingest, 'exhaust': exhaust})

        # Additional attributes for the celery backend
        self.share_annotate({
            'log_id': log.id,
            'source': log.source_config.source.long_title,
            'source_config': log.source_config.label,
        })

        # No need to lock, we've already acquired it here
        for datum in log.source_config.get_harvester().harvest_from_log(log, lock=False):
            if ingest and (datum.created or superfluous):
                transform.apply_async((datum.id, ))


@celery.shared_task(bind=True)
def schedule_harvests(self):
    with transaction.atomic():
        HarvestLog.objects.bulk_create([
            HarvestLog(
                end_date=datetime.date.today(),
                harvester_version=source_config.harvester.version,
                source_config=source_config,
                source_config_version=source_config.version,
                start_date=datetime.date.today() - datetime.timedelta(days=1),
            )
            for source_config in
            SourceConfig.objects.filter(disabled=False, source__is_deleted=False)
        ])
