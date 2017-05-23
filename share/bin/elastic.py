from share.bin.util import command


@command('Manage Elasticsearch')
def search(args, argv):
    """
    Usage: {0} search [--help] <command> [<args>...]

    Options:
        -h, --help     Show this screen.

    Commands:
    {1.subcommand_list}

    See '{0} elastic <command> --help' for more information on a specific command.
    """


@search.subcommand('Clear elasticsearch')
def purge(args, argv):
    """
    Usage: {0} elastic purge
    """
