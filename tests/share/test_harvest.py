from unittest import mock
import random
import threading
import uuid

from faker import Factory

import pytest

import pendulum

from django.conf import settings
from django.db import DatabaseError

from share.harvest.base import FetchResult
from share.harvest.exceptions import HarvesterConcurrencyError
from share.harvest.exceptions import HarvesterDisabledError
from share.models import Source
from share.models import HarvestLog
from share.models import RawDatum
from share import tasks
from share.harvest.scheduler import HarvestScheduler

from tests import factories


@pytest.fixture
def source_config():
    return factories.SourceConfigFactory()


@pytest.fixture
def mock_transform(monkeypatch):
    mock_transform = mock.Mock()
    monkeypatch.setattr('share.tasks.transform', mock_transform)
    return mock_transform


class SyncedThread(threading.Thread):

    def __init__(self, target, args=(), kwargs={}):
        self._end = threading.Event()
        self._start = threading.Event()

        def _target(*args, **kwargs):
            with target(*args, **kwargs):
                self._start.set()
                self._end.wait(10)

        super().__init__(target=_target, args=args, kwargs=kwargs)

    def start(self):
        super().start()
        self._start.wait(10)

    def join(self, timeout=1):
        self._end.set()
        return super().join(timeout)


@pytest.mark.django_db
def test_sources_have_access_tokens():
    for source in Source.objects.exclude(user__username=settings.APPLICATION_USERNAME)[:10]:
        assert source.user.authorization()


@pytest.mark.django_db
class TestHarvestTaskWithLog:

    def test_not_found(self):
        with pytest.raises(HarvestLog.DoesNotExist):
            tasks.harvest(log_id=12)

    # def test_disabled_source_config(self):
    #     with pytest.raises(HarvesterConcurrencyError):
    #         tasks.harvest(log_id=12)


@pytest.mark.django_db
class TestHarvestTask:

    @pytest.mark.parametrize('source_config_kwargs, task_kwargs, lock_config, exception', [
        ({}, {}, True, HarvesterConcurrencyError),
        ({'disabled': True}, {}, True, HarvesterDisabledError),
        ({'source__is_deleted': True}, {}, True, HarvesterDisabledError),
        ({'disabled': True, 'source__is_deleted': True}, {}, True, HarvesterDisabledError),
        ({'disabled': True}, {'ignore_disabled': True}, True, HarvesterConcurrencyError),
        ({'source__is_deleted': True}, {'ignore_disabled': True}, True, HarvesterConcurrencyError),
        ({'disabled': True, 'source__is_deleted': True}, {'ignore_disabled': True}, True, HarvesterConcurrencyError),
        ({'disabled': True}, {}, False, HarvesterDisabledError),
        ({'source__is_deleted': True}, {}, False, HarvesterDisabledError),
        ({'disabled': True, 'source__is_deleted': True}, {}, False, HarvesterDisabledError),
    ])
    def test_failure_cases(self, source_config_kwargs, task_kwargs, lock_config, exception):
        source_config = factories.SourceConfigFactory(**source_config_kwargs)
        log = factories.HarvestLogFactory(source_config=source_config)

        if lock_config:
            t = SyncedThread(source_config.acquire_lock)
            t.start()

        try:
            with pytest.raises(exception):
                tasks.harvest(log_id=log.id, **task_kwargs)
        finally:
            if lock_config:
                t.join()

    @pytest.mark.parametrize('source_config_kwargs, task_kwargs, lock_config', [
        ({}, {'force': True}, True),
        ({'disabled': True}, {'force': True}, True),
        ({'disabled': True}, {'force': True}, False),
        ({'disabled': True}, {'ignore_disabled': True}, False),
        ({'source__is_deleted': True}, {'ignore_disabled': True}, False),
        ({'source__is_deleted': True}, {'force': True}, False),
        ({'source__is_deleted': True}, {'force': True}, True),
    ])
    def test_overrides(self, source_config_kwargs, task_kwargs, lock_config):
        source_config = factories.SourceConfigFactory(**source_config_kwargs)
        log = factories.HarvestLogFactory(source_config=source_config)

        if lock_config:
            t = SyncedThread(source_config.acquire_lock)
            t.start()

        try:
            tasks.harvest(log_id=log.id, **task_kwargs)
        finally:
            if lock_config:
                t.join()

    def test_harvest_fails(self, source_config):
        source_config.harvester.get_class()._do_fetch.side_effect = ValueError('In a test')
        log = factories.HarvestLogFactory(source_config=source_config)

        with pytest.raises(ValueError) as e:
            tasks.harvest(log_id=log.id)

        log.refresh_from_db()

        assert e.value.args == ('In a test', )
        assert log.status == HarvestLog.STATUS.failed
        assert log.completions == 0
        assert 'ValueError: In a test' in log.context

    def test_harvest_database_error(self, source_config, mock_transform):
        log = factories.HarvestLogFactory(source_config=source_config)

        def _do_fetch(*args, **kwargs):
            yield ('doc1', b'doc1data')
            yield ('doc2', b'doc2data')
            yield ('doc3', b'doc3data')
            raise DatabaseError('In a test')
        source_config.harvester.get_class()._do_fetch = _do_fetch

        with pytest.raises(DatabaseError) as e:
            tasks.harvest(log_id=log.id)

        log.refresh_from_db()

        assert log.raw_data.count() == 3
        assert e.value.args == ('In a test', )
        assert log.status == HarvestLog.STATUS.failed
        assert log.completions == 0
        assert 'DatabaseError: In a test' in log.context
        assert mock_transform.apply_async.call_count == 3

    def test_partial_harvest_fails(self, source_config, mock_transform):
        log = factories.HarvestLogFactory(source_config=source_config)

        def _do_fetch(*args, **kwargs):
            yield ('doc1', b'doc1data')
            yield ('doc2', b'doc2data')
            yield ('doc3', b'doc3data')
            raise ValueError('In a test')
        source_config.harvester.get_class()._do_fetch = _do_fetch

        with pytest.raises(ValueError) as e:
            tasks.harvest(log_id=log.id)

        log.refresh_from_db()

        assert log.raw_data.count() == 3
        assert e.value.args == ('In a test', )
        assert log.status == HarvestLog.STATUS.failed
        assert log.completions == 0
        assert 'ValueError: In a test' in log.context
        assert mock_transform.apply_async.call_count == 3

    def test_log_values(self, source_config):
        task_id = uuid.uuid4()
        log = factories.HarvestLogFactory(source_config=source_config)

        tasks.harvest.apply((), {'log_id': log.id}, task_id=str(task_id), throw=True)

        log.refresh_from_db()

        assert log.task_id == task_id
        assert log.status == HarvestLog.STATUS.succeeded
        assert log.context == ''
        assert log.completions == 1
        assert log.source_config == source_config
        assert log.share_version == settings.VERSION
        assert log.harvester_version == source_config.get_harvester().VERSION
        assert log.source_config_version == source_config.version

    @pytest.mark.parametrize('count, rediscovered, superfluous, limit, ingest', {
        (count, int(rediscovered), False, int(limit) if limit is not None else None, True)
        for count in (0, 1, 500, 501, 1010)
        for limit in (None, 0, 1, count * .5, count, count * 2)
        for rediscovered in (0, 1, count * .5, count)
        if rediscovered <= count
    } | {
        (count, int(rediscovered), superfluous, None, ingest)
        for count in (0, 150)
        for ingest in (True, False)
        for superfluous in (True, False)
        for rediscovered in (0, count * .5, count)
        if rediscovered <= count
    })
    def test_data_flow(self, source_config, monkeypatch, count, rediscovered, superfluous, limit, ingest, django_assert_num_queries):
        assert rediscovered <= count, 'Y tho'

        fake = Factory.create()
        mock_ingest_task = mock.Mock()

        monkeypatch.setattr('share.tasks.transform', mock_ingest_task)
        source_config.harvester.get_class()._do_fetch.extend([(fake.sentence(), str(i * 50)) for i in range(count)])
        list(RawDatum.objects.store_chunk(source_config, (
            FetchResult(*tup) for tup in
            random.sample(source_config.harvester.get_class()._do_fetch, rediscovered))
        ))

        log = factories.HarvestLogFactory(source_config=source_config)

        tasks.harvest(log_id=log.id, superfluous=superfluous, limit=limit, ingest=ingest)

        log.refresh_from_db()

        assert log.completions == 1
        assert log.status == HarvestLog.STATUS.succeeded
        assert log.raw_data.count() == (count if limit is None or count < limit else limit)

        if limit is not None and rediscovered:
            assert RawDatum.objects.filter().count() >= rediscovered
            assert RawDatum.objects.filter().count() <= rediscovered + max(0, min(limit, count - rediscovered))
        else:
            assert RawDatum.objects.filter().count() == (count if limit is None or count < limit else limit)

        if ingest:
            if superfluous:
                assert mock_ingest_task.apply_async.call_count == min(count, limit or 99999)
            elif limit is not None:
                assert mock_ingest_task.apply_async.call_count <= min(limit, count)
                assert mock_ingest_task.apply_async.call_count >= min(limit, count) - rediscovered
            else:
                assert mock_ingest_task.apply_async.call_count == count - rediscovered
        else:
            assert mock_ingest_task.apply_async.call_count == 0

    def test_handles_duplicate_values(self, monkeypatch, source_config):
        fake = Factory.create()
        log = factories.HarvestLogFactory(source_config=source_config)

        source_config.harvester.get_class()._do_fetch.extend([(fake.sentence(), str(i * 50)) for i in range(100)] * 3)

        tasks.harvest(log_id=log.id, ingest=False)

        log.refresh_from_db()

        assert log.completions == 1
        assert log.status == HarvestLog.STATUS.succeeded
        assert log.raw_data.count() == 100

    def test_handles_duplicate_values_limit(self, monkeypatch, source_config):
        fake = Factory.create()
        log = factories.HarvestLogFactory(source_config=source_config)

        source_config.harvester.get_class()._do_fetch.clear()

        padding = []
        for _ in range(20):
            s = fake.sentence()
            padding.append((s, s * 5))

        for i in range(10):
            s = fake.sentence()
            source_config.harvester.get_class()._do_fetch.extend([(s, s * 5)] * 5)
            source_config.harvester.get_class()._do_fetch.extend(padding)

        tasks.harvest(log_id=log.id, limit=60, ingest=False)

        log.refresh_from_db()

        assert log.completions == 1
        assert log.status == HarvestLog.STATUS.succeeded
        assert log.raw_data.count() == 30

    def test_duplicate_data_different_identifiers(self, monkeypatch, source_config):
        source_config.harvester.get_class()._do_fetch.clear()
        source_config.harvester.get_class()._do_fetch.extend([
            ('identifier1', 'samedata'),
            ('identifier2', 'samedata'),
        ])

        with pytest.raises(ValueError) as e:
            list(source_config.get_harvester().harvest())

        assert e.value.args == ('<FetchResult(identifier2, None, b8bf83469c...)> has already been seen or stored with identifier "identifier1". Perhaps your identifier extraction is incorrect?', )

    def test_datestamps(self, source_config):
        source_config.harvester.get_class()._do_fetch.clear()
        source_config.harvester.get_class()._do_fetch.extend([
            ('identifier{}'.format(i), 'data{}'.format(i), pendulum.parse('2017-01-{}'.format(i)))
            for i in range(1, 10)
        ])

        for i, raw in enumerate(source_config.get_harvester().harvest_date_range(
            pendulum.parse('2017-01-01'),
            pendulum.parse('2017-02-01'),
        )):
            assert raw.datestamp is not None
            assert raw.datestamp.day == (i + 1)
            assert raw.datestamp.year == 2017

    def test_datestamps_out_of_range(self, source_config):
        source_config.harvester.get_class()._do_fetch.clear()
        source_config.harvester.get_class()._do_fetch.extend([
            ('identifier{}'.format(i), 'data{}'.format(i), pendulum.parse('2016-01-{}'.format(i)))
            for i in range(1, 10)
        ])

        with pytest.raises(ValueError) as e:
            list(source_config.get_harvester().harvest_date(pendulum.parse('2016-01-01')))

        assert e.value.args == ('result.datestamp is outside of the requested date range. 2016-01-03T00:00:00+00:00 from identifier3 is not within [2015-12-31T00:00:00+00:00 - 2016-01-01T00:00:00+00:00]', )

    def test_datestamps_within_24_hours(self, source_config):
        source_config.harvester.get_class()._do_fetch.clear()
        source_config.harvester.get_class()._do_fetch.extend([
            ('identifier{}'.format(timestamp), 'data{}'.format(timestamp), timestamp)
            for timestamp in (pendulum.parse('2016-01-01') - pendulum.parse('2016-01-03')).range('hours')
        ])

        list(source_config.get_harvester().harvest_date(pendulum.parse('2016-01-02')))

    @pytest.mark.parametrize('now, end_date, harvest_after, should_run', [
        (
            # Too early
            pendulum.parse('2017-01-01T00:00'),
            pendulum.parse('2017-01-01').date(),
            pendulum.parse('01:00').time(),
            False
        ),
        (
            # Just right
            pendulum.parse('2017-01-01T02:00'),
            pendulum.parse('2017-01-01').date(),
            pendulum.parse('01:00').time(),
            True
        ),
        (
            # Equal
            pendulum.parse('2017-01-01T01:00'),
            pendulum.parse('2017-01-01').date(),
            pendulum.parse('01:00').time(),
            True
        ),
        (
            # Way in the past
            pendulum.parse('2017-01-01T01:00'),
            pendulum.parse('2016-01-01').date(),
            pendulum.parse('01:00').time(),
            True
        ),
        (
            # In the future... ?
            pendulum.parse('2017-01-01T01:00'),
            pendulum.parse('2018-01-01').date(),
            pendulum.parse('01:00').time(),
            False
        ),
        (
            # Late harvester
            pendulum.parse('2017-01-01T01:00'),
            pendulum.parse('2017-01-01').date(),
            pendulum.parse('20:00').time(),
            False
        ),
    ])
    def test_harvest_after(self, monkeypatch, now, end_date, harvest_after, should_run, source_config):
        monkeypatch.setattr('share.tasks.harvest.apply_async', mock.Mock())

        source_config.harvest_after = harvest_after
        source_config.save()
        monkeypatch.setattr('django.utils.timezone.now', lambda: now)
        source_config.harvester.get_class()._do_fetch = mock.Mock(return_value=[])

        HarvestScheduler(source_config).date(end_date.add(days=-1))

        tasks.harvest()

        assert source_config.harvester.get_class()._do_fetch.called == should_run

    def test_latest_date(self):
        source_config = factories.SourceConfigFactory(
            full_harvest=True,
            earliest_date=pendulum.parse('2017-01-01').date()
        )

        # We have a harvest log with start_date equal to earliest_date
        # but a different source_config
        factories.HarvestLogFactory(
            start_date=pendulum.parse('2017-01-01').date(),
            end_date=pendulum.parse('2017-01-02').date(),
        )

        assert len(HarvestScheduler(source_config).all(cutoff=pendulum.parse('2018-01-01').date())) == 365

    def test_caught_up(self):
        source_config = factories.SourceConfigFactory(
            full_harvest=True,
            earliest_date=pendulum.parse('2017-01-01').date()
        )

        factories.HarvestLogFactory(
            source_config=source_config,
            start_date=pendulum.parse('2017-01-01').date(),
            end_date=pendulum.parse('2017-01-02').date(),
        )

        factories.HarvestLogFactory(
            source_config=source_config,
            start_date=pendulum.parse('2018-01-01').date(),
            end_date=pendulum.parse('2018-01-02').date(),
        )

        assert len(HarvestScheduler(source_config).all(cutoff=pendulum.parse('2018-01-01').date())) == 0

    def test_latest_date_null(self):
        source_config = factories.SourceConfigFactory(
            full_harvest=True,
            earliest_date=pendulum.parse('2017-01-01').date()
        )
        assert len(HarvestScheduler(source_config).all(cutoff=pendulum.parse('2018-01-01').date())) == 365

    def test_obsolete(self):
        source_config = factories.SourceConfigFactory()

        hlv1 = factories.HarvestLogFactory(
            harvester_version=source_config.harvester.version,
            source_config=source_config,
            start_date=pendulum.parse('2017-01-01').date(),
        )

        old_version = source_config.harvester.get_class().VERSION
        source_config.harvester.get_class().VERSION += 1
        new_version = source_config.harvester.get_class().VERSION

        hlv2 = factories.HarvestLogFactory(
            harvester_version=source_config.harvester.version,
            source_config=source_config,
            start_date=pendulum.parse('2017-01-01').date(),
        )

        tasks.harvest(log_id=hlv2.id)
        tasks.harvest(log_id=hlv1.id)

        hlv1.refresh_from_db()
        hlv2.refresh_from_db()

        assert hlv2.status == HarvestLog.STATUS.succeeded
        assert hlv2.harvester_version == new_version

        assert hlv1.status == HarvestLog.STATUS.skipped
        assert hlv1.harvester_version == old_version
        assert hlv1.context == HarvestLog.SkipReasons.obsolete.value

    @pytest.mark.parametrize('completions, status, new_version, updated', [
        (0, HarvestLog.STATUS.created, 2, True),
        (1, HarvestLog.STATUS.created, 2, False),
        (88, HarvestLog.STATUS.created, 2, False),
        (88, HarvestLog.STATUS.failed, 2, False),
        (0, HarvestLog.STATUS.failed, 2, True),
        (0, HarvestLog.STATUS.succeeded, 2, True),
    ])
    def test_autoupdate(self, completions, status, new_version, updated):
        source_config = factories.SourceConfigFactory()

        source_config.harvester.get_class().VERSION = 1

        hl = factories.HarvestLogFactory(
            status=status,
            completions=completions,
            harvester_version=source_config.harvester.version,
            source_config=source_config,
            start_date=pendulum.parse('2017-01-01').date(),
        )

        source_config.harvester.get_class().VERSION = new_version

        tasks.harvest(log_id=hl.id)

        hl.refresh_from_db()

        if updated:
            assert hl.status == HarvestLog.STATUS.succeeded
        elif new_version > 1:
            assert hl.status == HarvestLog.STATUS.skipped
            assert hl.context == HarvestLog.SkipReasons.obsolete.value

        assert (hl.harvester_version == new_version) == updated
