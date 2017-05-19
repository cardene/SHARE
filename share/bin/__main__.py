"""
Usage: share [--version] [--help] <command> [<args>...]

Options:
    -h, --help     Show this screen.
    -v, --version  Show version.

Commands:
{}

See 'share <command> --help' for more information on a specific command.
"""

import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'project.settings')  # noqa
django.setup()  # noqa

import sys

from docopt import docopt

from django.conf import settings

from share.bin import debug  # noqa
from share.bin import info  # noqa
from share.bin import ingest  # noqa
from share.bin.util import COMMANDS


spaces = (4 + max(len(k) for k in COMMANDS.keys()))

args = docopt(
    __doc__.format('\n'.join(
        '    {{:{}}}{{}}'.format(spaces).format(key, value.description)
        for key, value in sorted(COMMANDS.items(), key=lambda x: x[0]))
    ),
    version=settings.VERSION,
    options_first=True
)

try:
    func = COMMANDS[args['<command>']]
except KeyError:
    print('Invalid command "{}"'.format(args['<command>']))
    sys.exit(1)

func([args['<command>']] + args['<args>'])
