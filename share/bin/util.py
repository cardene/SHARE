import functools

from django.conf import settings
from docopt import docopt


COMMANDS = {}


def command(description):
    def _command(func):
        if func.__name__ in COMMANDS:
            raise ValueError('{} already defined'.format(func.__name__))

        @functools.wraps(func)
        def inner(argv):
            parsed = docopt(func.__doc__, version=settings.VERSION, argv=argv)
            return func(parsed, argv)

        COMMANDS[func.__name__] = inner
        inner.description = description

        return inner
    return _command
