"""
Command-line parameters.
"""

import os
import re
import sys

import click
import humanfriendly
from click import ClickException

from .utils import parse_timespan


class ListParamType(click.ParamType):
    """
    Command-line parameter type for lists. It supports reading from file and stdin.
    """

    name = 'list'

    def __init__(self, type=None, separator=r'[,\s]+'):
        self.type = type
        self.separator = separator

    def convert(self, value, param, ctx):
        value = _preprocess_value(value)
        result = [v.strip() for v in re.split(self.separator, value) if v]

        if self.type:
            if isinstance(self.type, click.ParamType):
                result = [self.type(v, param=param, ctx=ctx) for v in result]
            else:
                result = [self.type(v) for v in result]

        return result


class StringParamType(click.ParamType):
    """
    Command-line parameter type for string values. It supports reading from file and stdin.
    """

    name = 'string'

    def convert(self, value, param, ctx):
        return _preprocess_value(value)


class TimeSpanParamType(click.ParamType):
    """
    Command-line parameter type for times span values.
    """

    name = 'timespan'

    def convert(self, value, param, ctx):
        try:
            return parse_timespan(value)
        except humanfriendly.InvalidTimespan as e:
            raise ClickException(f'Invalid timespan value for the parameter "{param.name}": {str(e)}')


class BytesParamType(click.ParamType):
    """
    Command-line parameter type for bytes values.
    """

    name = 'bytes'

    def convert(self, value, param, ctx):
        if isinstance(value, str):
            value = value.strip()
            if value.startswith('-'):
                value = value[1:]
                multiplier = -1
            else:
                multiplier = 1

            return multiplier * humanfriendly.parse_size(value, binary=True)

        return value


def _preprocess_value(value):
    """
    Preprocess command-line parameter value. It adds support of reading from file and stdin.
    """
    if value == '-':
        return sys.stdin.read()

    if value.startswith('@'):
        with open(os.path.expanduser(value[1:])) as f:
            return f.read()

    return value
