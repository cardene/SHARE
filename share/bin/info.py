from share.models import SourceConfig
from share.bin.util import command


@command('List SourceConfigs')
def sources(args, argv):
    """
    Usage: {0} sources [-e | -d]

    Options:
        -e, --enabled        Only list enabled SourceConfigs
        -d, --disabled       Only list disabled SourceConfigs

    Print out a list of currently installed source configs
    """
    configs = SourceConfig.objects.all()

    if args['--enabled']:
        configs = configs.filter(enabled=True, source__is_deleted=False)

    if args['--disabled']:
        configs = configs.exclude(enabled=True, source__is_deleted=False)

    for config in configs.values_list('label', flat=True):
        print(config)
