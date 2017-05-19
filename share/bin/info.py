from share.models import SourceConfig
from share.bin.util import command


@command('Print all SourceConfigs')
def sources(args, argv):
    """
    Usage: share sources

    Print out a list of currently installed source configs
    """
    for config in SourceConfig.objects.values_list('label', flat=True):
        print(config)
