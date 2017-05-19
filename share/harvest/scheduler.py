import pendulum

from django.db import models

from share.models import HarvestLog


class HarvestScheduler:
    """Utility class for creating HarvestLogs

    All date ranges are treated as [start, end)

    """

    def __init__(self, source_config):
        self.source_config = source_config

    def all(self, cutoff=None, allow_backharvest=True, **kwargs):
        if cutoff is None:
            cutoff = pendulum.utcnow().date()

        if not hasattr(self.source_config, 'latest'):
            latest_date = HarvestLog.objects.filter(source_config=self.source_config).aggregate(models.Max('end_date'))
        else:
            latest_date = self.source_config.latest

        if not latest_date and allow_backharvest and self.source_config.backharvesting and self.source_config.earliest_date:
            latest_date = self.source_config.earliest_date
        elif not latest_date:
            latest_date = cutoff - self.source_config.harvest_interval

        return self.range(latest_date, cutoff, **kwargs)

    def today(self, **kwargs):
        return self.date(pendulum.today().date(), **kwargs)[0]

    def yesterday(self, **kwargs):
        return self.date(pendulum.yesterday().date(), **kwargs)[0]

    def date(self, date, **kwargs):
        return self.range(date, date.add(days=1), **kwargs)[0]

    def range(self, start, end, save=True):
        if start >= end:
            raise ValueError('start must be less than end. {!r} >= {!r}'.format(start, end))

        logs = []

        log_kwargs = {
            'source_config': self.source_config,
            'source_config_version': self.source_config.version,
            'harvester_version': self.source_config.get_harvester().VERSION,
        }

        sd, ed = start, start

        while ed + self.source_config.harvest_interval <= end:
            sd, ed = ed, ed + self.source_config.harvest_interval
            logs.append(HarvestLog(start_date=sd, end_date=ed, **log_kwargs))

        if save:
            HarvestLog.objects.bulk_create(logs)

        return logs
