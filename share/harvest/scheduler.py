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
        """
        Args:
            cutoff (date, optional): The upper bound to schedule harvests to. Default to today.
            allow_backharvest (bool, optional): Allow a SourceConfig to generate a full back harvest. Defaults to True.
                The SourceConfig must be marked as backharvestable and have earliest_date set.
            **kwargs: Forwarded to .range

        Returns:
            A list of harvest logs

        """
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
        """
        Functionally the same as calling .range(today, tomorrow)[0].
        You probably want to use .yesterday rather than .today.

        Args:
            **kwargs: Forwarded to .date

        Returns:
            A single Harvest log that *includes* today.

        """
        return self.date(pendulum.today().date(), **kwargs)

    def yesterday(self, **kwargs):
        """
        Functionally the same as calling .range(yesterday, today)[0].

        Args:
            **kwargs: Forwarded to .date

        Returns:
            A single Harvest log that *includes* yesterday.

        """
        return self.date(pendulum.yesterday().date(), **kwargs)

    def date(self, date, **kwargs):
        """
        Args:
            date (date):
            **kwargs: Forwarded to .range

        Returns:
            A single Harvest log that *includes* date.

        """
        return self.range(date, date.add(days=1), **kwargs)[0]

    def range(self, start, end, save=True):
        """

        Args:
            start (date):
            end (date):
            save (bool, optional): If True, attempt to save the created HarvestLogs. Defaults to True.

        Returns:
            A list of HarvestLogs within [start, end).

        """
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

        if logs and save:
            return HarvestLog.objects.bulk_get_or_create(logs)

        return logs
