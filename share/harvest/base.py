from typing import Iterator
import abc
import collections
import datetime
import logging
import types

import pendulum
import requests

from django.utils import timezone
from django.db import transaction

from share.harvest.exceptions import HarvesterConcurrencyError
from share.harvest.exceptions import HarvesterDisabledError
from share.harvest.ratelimit import RateLimittedProxy
from share.harvest.serialization import EverythingSerializer
from share.models import RawDatum


logger = logging.getLogger(__name__)
FetchResult = collections.namedtuple('FetchResult', ('identifier', 'datum'))


class BaseHarvester(metaclass=abc.ABCMeta):
    """

    Fetch:
        Aquire and serialize data from a remote source, respecting rate limits.
        fetch* methods return a generator that yield FetchResult objects

    Harvest:
        Fetch and store data, respecting global rate limits.
        harvest* methods return a generator that yield RawDatum objects

    """

    SERIALIZER_CLASS = EverythingSerializer

    def __init__(self, source_config, pretty=False, **kwargs):
        """

        Args:
            source_config (SourceConfig):
            pretty (bool, optional): Defaults to False.
            **kwargs: Custom kwargs, generally from the source_config. Stored in self.kwargs

        """
        self.kwargs = kwargs
        self.config = source_config
        self.serializer = self.SERIALIZER_CLASS(pretty)

        # TODO Add custom user agent
        self.requests = RateLimittedProxy(requests.Session(), self.config.rate_limit_allowance, self.config.rate_limit_period)

    def fetch_id(self, identifier: str) -> FetchResult:
        """Fetch a document by provider ID.

        Optional to implement, intended for dev and manual testing.

        Args:
            identifier (str): Unique ID the provider uses to identify works.

        Returns:
            FetchResult

        """
        raise NotImplementedError('{!r} does not support fetching by ID'.format(self))

    def fetch(self, **kwargs):
        """Fetch data from today.

        Yields:
            FetchResult

        """
        return self.fetch_date_range(datetime.date.today() - datetime.timedelta(days=1), datetime.date.today(), **kwargs)

    def fetch_date(self, date: datetime.date, **kwargs):
        """Fetch data from the specified date.

        Yields:
            FetchResult
        """
        return self.fetch_date_range(date - datetime.timedelta(days=1), date, **kwargs)

    def fetch_date_range(self, start: datetime.date, end: datetime.date, limit=None, **kwargs):
        """Fetch data from the specified date range.

        Yields:
            FetchResult

        """
        if not isinstance(start, datetime.date):
            raise TypeError('start must be a datetime.date. Got {!r}'.format(start))

        if not isinstance(end, datetime.date):
            raise TypeError('end must be a datetime.date. Got {!r}'.format(end))

        if start >= end:
            raise ValueError('start must be before end. {!r} > {!r}'.format(start, end))

        if limit == 0:
            return  # No need to do anything

        # Cast to datetimes for compat reasons
        start = pendulum.Pendulum.instance(datetime.datetime.combine(start, datetime.time(0, 0, 0, 0, timezone.utc)))
        end = pendulum.Pendulum.instance(datetime.datetime.combine(end, datetime.time(0, 0, 0, 0, timezone.utc)))

        # TODO Remove me in 2.9.0
        if hasattr(self, 'shift_range'):
            logger.warning('%r implements a deprecated interface. Handle date transforms in _do_fetch. shift_range will no longer be called in 2.9.0')
            start, end = self.shift_range(start, end)

        data_gen = self._do_fetch(start, end, **kwargs)

        if not isinstance(data_gen, types.GeneratorType) and len(data_gen) != 0:
            raise TypeError('{!r}._do_fetch must return a GeneratorType for optimal performance and memory usage'.format(self))

        for i, (identifier, datum) in enumerate(data_gen):
            yield FetchResult(identifier, self.serializer.serialize(datum))

            if limit is not None and i >= limit:
                break

    def harvest_id(self, identifier) -> RawDatum:
        """Harvest a document by ID.

        Note:
            Dependant on whether or not fetch_id is implemented.

        Args:
            identifier (str): Unique ID the provider uses to identify works.

        Returns:
            RawDatum

        """
        res = self.fetch_by_id(identifier)
        return RawDatum.objects.store_data(res.identifier, res.datum, self.config)

    def harvest(self, **kwargs):
        """Fetch data from today.

        Yields:
            RawDatum

        """
        return self.harvest_date_range(datetime.date.today() - datetime.timedelta(days=1), datetime.date.today(), **kwargs)

    def harvest_date(self, date: datetime.date, **kwargs):
        """Harvest data from the specified date.

        Yields:
            RawDatum

        """
        return self.fetch_date_range(date - datetime.timedelta(days=1), date, **kwargs)

    def harvest_date_range(self, start: datetime.date, end: datetime.date, limit=None, force=False, ignore_disabled=False, lock=True, **kwargs):
        """Fetch data from the specified date range.

        Args:
            limit (int, optional): The maximum number of unique data to harvest. Defaults to None.
            force (bool, optional): Disable all safety checks, unexpected exceptions will still be raised. Defaults to False.
            ignore_disabled (bool, optional): Don't check if this Harvester or Source is disabled or deleted. Defaults to False.
            lock (bool, optional): Lock the SourceConfig before harvesting to prevent rate limit violations. Defaults to True.
            **kwargs: Forwared to _do_fetch.

        Yields:
            RawDatum

        """
        if self.serializer.pretty:
            raise ValueError('To ensure that data is optimally deduplicated, harvests may not occur while using a pretty serializer.')

        if (self.config.disabled or self.config.source.is_deleted) and not (force or ignore_disabled):
            raise HarvesterDisabledError('Harvester {!r} is disabled. Either enable it, run with force=True, or ignore_disabled=True.'.format(self.config))

        with transaction.atomic(using='locking'):
            if lock:
                try:
                    self.config.acquire_lock(using='locking')
                except HarvesterConcurrencyError as e:
                    if not force:
                        raise e
                    logger.warning('force = True, proceeding without SourceConfig lock.')

            logger.info('Harvesting %s - %s from %r', start, end, self.config)
            yield from RawDatum.objects.store_chunk(self.config, self.fetch_date_range(start, end, **kwargs), limit=limit)

    def harvest_from_log(self, harvest_log, **kwargs):
        """Harvest data as specified by the given harvest_log.

        Args:
            harvest_log (HarvestLog): The HarvestLog that describes the parameters of this harvest
            limit (int, optional): The maximum number of unique data to harvest. Defaults to None.
            **kwargs: Forwared to harvest_date_range.

        Yields:
            RawDatum

        """
        error = None
        datum_ids = []
        logger.info('Harvesting %r', harvest_log)

        with harvest_log.handle(self.VERSION):
            try:
                for datum in self.harvest_date_range(harvest_log.start_date, harvest_log.end_date, **kwargs):
                    datum_ids.append(datum.id)
                    yield datum
            except Exception as e:
                error = e
                raise error
            finally:
                try:
                    harvest_log.raw_data.add(*datum_ids)
                except Exception as e:
                    logger.exception('Failed to connection %r to raw data', harvest_log)
                    # Avoid shadowing the original error
                    if not error:
                        raise e

    def _do_fetch(self, start: datetime.datetime, end: datetime.datetime, **kwargs) -> Iterator[FetchResult]:
        """Fetch date from this source inside of the given date range.

        The given date range should be treated as [start, end)

        Any HTTP[S] requests MUST be sent using the self.requests client.
        It will automatically enforce rate limits

        Args:
            start_date (datetime): Date to start fetching data from, inclusively.
            end_date (datetime): Date to fetch data up to, exclusively.
            **kwargs: Arbitrary kwargs passed to subclasses, used to customize harvesting.

        Returns:
            Iterator<FetchResult>: The fetched data.

        """
        if hasattr(self, 'do_harvest'):
            logger.warning('%r implements a deprecated interface. do_harvest has been replaced by _do_fetch for clarity', self)
            return self.do_harvest(start, end, **kwargs)

        raise NotImplementedError()
