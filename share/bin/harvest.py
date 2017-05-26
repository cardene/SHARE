import re
import os

import pendulum

from share import tasks
from share.bin.util import command
from share.harvest.scheduler import HarvestScheduler
from share.models import SourceConfig


@command('Fetch data to disk or stdout, using the specified SourceConfig')
def fetch(args, argv):
    """
    Usage: {0} fetch <sourceconfig> [<date> | --start=YYYY-MM-DD --end=YYYY-MM-DD] [--limit=LIMIT] [--print | --out=DIR] [--set-spec=SET]

    Options:
        -l, --limit=NUMBER      Limit the harvester to NUMBER of documents
        -p, --print             Print results to stdout rather than to a file
        -o, --out=DIR           The directory to store the fetched data in. Defaults to ./fetched/<sourceconfig>
        -s, --start=YYYY-MM-DD  The date at which to start fetching data.
        -e, --end=YYYY-MM-DD    The date at which to stop fetching data.
        -i, --ignore-disabled   Allow disabled SourceConfigs to run.
        --set-spec=SET          The OAI setSpec to limit harvesting to.
    """

    try:
        config = SourceConfig.objects.get(label=(args['<sourceconfig>']))
    except SourceConfig.DoesNotExist:
        print('SourceConfig "{}" not found.'.format(args['<sourceconfig>']))
        return -1

    harvester = config.get_harvester(pretty=True)

    kwargs = {k: v for k, v in {
        'limit': int(args['--limit']) if args.get('--limit') else None,
        'set_spec': args.get('--set-spec'),
        'ignore_disabled': args.get('ignore_disabled'),
    }.items() if v is not None}

    if not args['<date>'] and not (args['--start'] and args['--end']):
        gen = harvester.fetch(**kwargs)
    elif args['<date>']:
        gen = harvester.fetch_date(pendulum.parse(args['<date>']), **kwargs)
    else:
        gen = harvester.fetch_date_range(pendulum.parse(args['--start']), pendulum.parse(args['--end']), **kwargs)

    if not args['--print']:
        args['--out'] = args['--out'] or os.path.join(os.curdir, 'fetched', config.label)
        os.makedirs(args['--out'], exist_ok=True)

    for result in gen:
        if args['--print']:
            print('Harvested data with identifier "{}"'.format(result.identifier))
            print(result.datum)
            print('\n')
        else:
            suffix = '.xml' if result.datum.startswith('<') else '.json'
            with open(os.path.join(args['--out'], re.sub(r'[:\\\/\?\*]', '', str(result.identifier))) + suffix, 'w') as fobj:
                fobj.write(result.datum)


@command('Harvest data using the specified SourceConfig')
def harvest(args, argv):
    """
    Usage:
        {0} harvest <sourceconfig> [<date>] [-nflj] [options]
        {0} harvest <sourceconfig> [<date>] [-afsj] [options]
        {0} harvest <sourceconfig> --all [<date>] [-afsj | -nflj] [options]
        {0} harvest <sourceconfig> (--start=YYYY-MM-DD> --end=YYYY-MM-DD) [-afsj | -nflj] [options]

    Options:
        -l, --limit=NUMBER      Limit the harvester to NUMBER of documents
        -s, --start=YYYY-MM-DD  The date at which to start fetching data.
        -e, --end=YYYY-MM-DD    The date at which to stop fetching data.
        --set-spec=SET          The OAI setSpec to limit harvesting to.
    """
    try:
        config = SourceConfig.objects.get(label=(args['<sourceconfig>']))
    except SourceConfig.DoesNotExist:
        print('SourceConfig "{}" not found.'.format(args['<sourceconfig>']))
        return -1

    kwargs = {k: v for k, v in {
        'limit': int(args['--limit']) if args.get('--limit') else None,
        'set_spec': args.get('--set-spec'),
    }.items() if v is not None}

    if not args['<date>'] and not (args['--start'] and args['--end']):
        gen = config.get_harvester().harvest(**kwargs)
    elif args['<date>']:
        gen = config.get_harvester().harvest_date(pendulum.parse(args['<date>']), **kwargs)
    else:
        gen = config.get_harvester().harvest_date_range(pendulum.parse(args['--start']), pendulum.parse(args['--end']), **kwargs)

    list(gen)


@command('Create harvestlogs for the specified SourceConfig')
def schedule(args, argv):
    """
    Usage:
        {0} schedule <sourceconfig> [<date> | (--start=YYYY-MM-DD --end=YYYY-MM-DD) | --all] [--tasks | --run]

    Options:
        -t, --tasks             Spawn harvest tasks for each created log.
        -r, --run               Run the harvest task for each created log.
        -a, --all               Schedule all logs between today and the SourceConfig's earliest date.
        -s, --start=YYYY-MM-DD  The date at which to start fetching data.
        -e, --end=YYYY-MM-DD    The date at which to stop fetching data.
        -j, --no-ingest         Do not process harvested data.
    """
    try:
        config = SourceConfig.objects.get(label=(args['<sourceconfig>']))
    except SourceConfig.DoesNotExist:
        print('SourceConfig "{}" not found.'.format(args['<sourceconfig>']))
        return -1

    kwargs = {k: v for k, v in {
        'ingest': not args.get('--no-ingest'),
    }.items() if v is not None}

    scheduler = HarvestScheduler(config)

    if not (args['<date>'] or args['--start'] or args['--end']):
        logs = [scheduler.today()]
    elif args['<date>']:
        logs = [scheduler.date(pendulum.parse(args['<date>']))]
    else:
        logs = scheduler.range(pendulum.parse(args['--start']), pendulum.parse(args['--end']))

    for log in logs:
        if args['--run']:
            tasks.harvest.apply((), {'log_id': log.id, **kwargs})
        elif args['--tasks']:
            tasks.harvest.apply_async((), {'log_id': log.id, **kwargs})
