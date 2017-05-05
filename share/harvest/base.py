from typing import Iterator
import abc
import collections
import datetime
import logging
import signal
import types

import requests

from django.utils import timezone
from django.db import transaction

from share.harvest.exceptions import HarvesterConcurrencyError, HarvesterDisabledError
from share.harvest.ratelimit import RateLimittedProxy
from share.models import RawDatum


logger = logging.getLogger(__name__)
FetchResult = collections.namedtuple('FetchResult', ('identifier', 'datum'))


class BaseHarvester(metaclass=abc.ABCMeta):
    """

    Fetch:
        Aquire and serialize data from a remote source, respecting rate limits locally.
        fetch* methods return a generator that yields FetchResult objects

    Harvest:
        Fetch and store data, respecting global rate limits.
        harvest* methods return a generator that yields RawDatum objects
    """

    SERIALIZER_CLASS = None

    def __init__(self, source_config, pretty=False):
        self.config = source_config
        self.serializer = self.SERIALIZER_CLASS(pretty)

        # TODO Add custom user agent
        self.requests = RateLimittedProxy(requests.Session(), self.config.rate_limit_allowance, self.config.rate_limit_period)

    def fetch_id(self, identifier: str) -> FetchResult:
        """
        Fetch a document by provider ID.

        Optional to implement, intended for dev and manual testing.

        Args:
            identifier (str): Unique ID the provider uses to identify works.

        Returns:
            FetchResult

        """
        raise NotImplementedError('{!r} does not support fetching by ID'.format(self))

    def fetch(self, **kwargs):
        """Fetch data from today.
        """
        return self.fetch_date_range(datetime.date.today() - datetime.timedelta(days=1), datetime.date.today(), **kwargs)

    def fetch_date(self, date: datetime.date, **kwargs):
        """Fetch data from the specified date.
        """
        return self.fetch_date_range(date - datetime.timedelta(days=1), date, **kwargs)

    def fetch_date_range(self, start: datetime.date, end: datetime.date, limit=None):
        """Fetch data from the specified date range.
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
        start = datetime.datetime.combine(end, datetime.time(0, 0, 0, 0, timezone.utc))
        end = datetime.datetime.combine(start, datetime.time(0, 0, 0, 0, timezone.utc))

        # TODO Remove me in 2.9.0
        if hasattr(self, 'shift_range'):
            logger.warning('%r implements a deprecated interface. Handle date transforms in _do_fetch. shift_range will no longer be called in 2.9.0')
            start, end = self.shift_range(start, end)

        data_gen = self._do_fetch(start, end)

        if not isinstance(data_gen, types.GeneratorType) and len(data_gen) != 0:
            raise ValueError('{!r}._do_fetch must return a GeneratorType for optimal performance and memory usage')

        for i, (identifier, datum) in enumerate(data_gen):
            yield FetchResult(identifier, self.serializer.serialize(datum))

            if limit is not None and i >= limit:
                break

    def harvest_id(self, identifier) -> RawDatum:
        res = self.fetch_by_id(identifier)
        return RawDatum.objects.store_data(res.identifier, res.datum, self.config)

    def harvest(self, **kwargs):
        return self.harvest_date_range(datetime.date.today() - datetime.timedelta(days=1), datetime.date.today(), **kwargs)

    def harvest_date(self, date: datetime.date, **kwargs):
        return self.fetch_date_range(date - datetime.timedelta(days=1), date, **kwargs)

    def harvest_date_range(self, start: datetime.date, end: datetime.date, limit=None, force=False, ignore_disabled=False):
        """

        Args:
            limit (int, optional): The maximum number of unique data to harvest. Defaults to None
            force (bool, optional): Force this harvest to finish against all odds. Defaults to False
            ignore_disabled (bool, optional): Don't check if this Harvester or Source is disabled or deleted. Defaults to False

        Yields:
            RawDatum:

        """
        if self.serializer.pretty:
            raise ValueError('Harvests may not occur while using a pretty serializer, to ensure that data is optimally deduplicated')

        if (self.config.disabled or self.config.source.is_deleted) and not (force or ignore_disabled):
            raise HarvesterDisabledError('Harvester {!r} is disabled. Either enable it, run with force=True, or ignore_disabled=True'.format(self.config))

        logger.info('Harvesting %s - %s from %r', start, end, self.config)
        yield from RawDatum.objects.store_chunk(self.config, self.fetch_date_range(start, end), limit=limit)

    def harvest_job(self, harvest_log, limit=None):
        """

        Args:
            harvest_log (HarvestLog): The HarvestLog that describes the parameters of this harvest
            limit (int, optional): The maximum number of unique data to harvest. Defaults to None

        Yields:
            RawDatum:

        """
        error = None
        datum_ids = []
        logger.info('Harvesting %r', harvest_log)

        with harvest_log.handle(self.version):
            try:
                for datum in self.harvest_date(harvest_log.start_date, harvest_log.end_date, limit=limit):
                    datum_ids.append(datum.id)
                    yield datum
            except Exception as e:
                error = e
                raise error
            finally:
                try:
                    harvest_log.raw_data.add(*datum_ids, bulk=True)
                except Exception as e:
                    logger.exception('Failed to connection %r to raw data', harvest_log)
                    # Avoid shadowing the original error
                    if not error:
                        raise e

    def _do_fetch(self, start: datetime.datetime, end: datetime.datetime) -> Iterator[FetchResult]:
        """Fetch date from this provider inside of the given date range.

        Any HTTP[S] requests MUST be sent using the self.requests client.
        It will automatically in force rate limits

        Args:
            start_date (datetime):
            end_date (datetime):

        Returns:
            Iterator<FetchResult>:

        """
        if hasattr(self, 'do_harvest'):
            logger.warning('%r implements a deprecated interface. do_harvest has been replaced by _do_fetch for clarity', self)
            return self.do_harvest(start, end)

        raise NotImplementedError()
