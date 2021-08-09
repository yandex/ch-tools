import json
import os
import re
import sys
import subprocess
from collections import OrderedDict
from datetime import datetime, timedelta
from json import JSONDecodeError
from typing import Mapping

import humanfriendly
from click import ClickException, ParamType
from psycopg2.extras import DictRow, Range
from pygments import highlight
from pygments.formatters.terminal256 import Terminal256Formatter
from pygments.lexers.data import JsonLexer, YamlLexer
from pygments.style import Style
from pygments.token import Token
from tabulate import tabulate

from .yaml import dump_yaml


class ListParamType(ParamType):
    """
    Command-line parameter type for lists.
    """
    name = 'list'

    def __init__(self, type=None, separator=r'[,\s]+'):
        self.type = type
        self.separator = separator

    def convert(self, value, param, ctx):
        value = self._preprocess(value)
        result = [v.strip() for v in re.split(self.separator, value)]

        if self.type:
            if isinstance(self.type, ParamType):
                result = [self.type(v, param=param, ctx=ctx) for v in result]
            else:
                result = [self.type(v) for v in result]

        return result

    @staticmethod
    def _preprocess(value):
        if value == '-':
            return sys.stdin.read()

        if value.startswith('@'):
            with open(os.path.expanduser(value[1:])) as f:
                return f.read()

        return value


class JsonParamType(ParamType):
    """
    Command-line parameter type for JSON values.
    """
    name = 'json'

    def convert(self, value, param, ctx):
        try:
            return json.loads(self._preprocess(value))
        except JSONDecodeError as e:
            raise ClickException(f'Invalid JSON value for the parameter "{param.name}". {str(e)}')

    @staticmethod
    def _preprocess(value):
        if value == '-':
            return sys.stdin.read()

        if value.startswith('@'):
            with open(os.path.expanduser(value[1:])) as f:
                return f.read()

        return value


class BytesParamType(ParamType):
    """
    Command-line parameter type for bytes values.
    """
    name = 'bytes'

    def convert(self, value, param, ctx):
        return humanfriendly.parse_size(value, binary=True)


class FormatStyle(Style):
    styles = {
        Token.Name.Tag: 'bold ansibrightblue',
        Token.Punctuation: 'bold ansiwhite',
        Token.String: 'ansigreen',
    }


def ensure_no_unsupported_options(ctx, kwargs, unsupported_options, msg):
    for arg_name, option_name in unsupported_options.items():
        if kwargs.get(arg_name):
            ctx.fail(msg.format(option_name))


def print_response(ctx,
                   value,
                   format=None,
                   field_formatters=None,
                   table_formatter=None,
                   table_keys=None,
                   ignored_keys=None,
                   quiet=None,
                   id_key=None,
                   separator=None):
    if format is None:
        format = ctx.obj.get('format')
        if format is None:
            format = 'table' if (table_formatter or table_keys) else 'json'

    value = purify_value(value, formatters=field_formatters, ignored_keys=ignored_keys)

    if format == 'table':
        if table_keys:
            table_formatter = lambda obj: OrderedDict((key, obj[key]) for key in table_keys)

        if table_formatter:
            value = [table_formatter(v) for v in value]

    if quiet:
        if id_key is None:
            id_key = 'id'

        if separator is None:
            separator = ','
        else:
            separator = separator.replace(r'\n', '\n')

        if isinstance(value, list):
            print(separator.join(item[id_key] for item in value))
        else:
            print(value[id_key])

    elif format == 'table':
        print_table(value)

    elif format == 'yaml':
        print_yaml(value)

    else:
        print_json(value)


def purify_value(value, formatters=None, ignored_keys=None):
    if isinstance(value, (DictRow, Mapping)):
        result = OrderedDict()
        for key, value in value.items():
            if ignored_keys and key in ignored_keys:
                continue

            formatter = formatters.get(key) if formatters else None
            value = formatter(value) if formatter else purify_value(value)

            result[key] = value

        return result

    if isinstance(value, list):
        return [purify_value(v, formatters=formatters, ignored_keys=ignored_keys) for v in value]

    if isinstance(value, Range):
        return f'{value.lower}-{value.upper}' if value else None

    if isinstance(value, datetime):
        return value.isoformat(' ', 'seconds')

    if isinstance(value, timedelta):
        return str(value)

    return value


def print_json(value):
    json_dump = json.dumps(value, indent=2)
    if sys.stdout.isatty():
        print(highlight(json_dump, JsonLexer(), Terminal256Formatter(style=FormatStyle)), end='')
    else:
        print(json_dump)


def print_yaml(value):
    yaml_dump = dump_yaml(value)
    if sys.stdout.isatty():
        print(highlight(yaml_dump, YamlLexer(), Terminal256Formatter(style=FormatStyle)), end='')
    else:
        print(yaml_dump)


def print_table(value):
    print(tabulate(value, headers='keys'))


def format_bytes(value):
    if value > 0:
        return humanfriendly.format_size(value, binary=True)
    elif value < 0:
        return '-{0}'.format(humanfriendly.format_size(-value, binary=True))
    else:
        return '0'


def format_bytes_per_second(value):
    if value != 0:
        return f'{format_bytes(value)}/s'
    else:
        return '0'


def execute(command, *, host=None, input=None, binary=False):
    if host:
        command = 'ssh {0} {1}'.format(host, command)

    proc = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if isinstance(input, str):
        input = input.encode()

    stdout, stderr = proc.communicate(input=input)

    if proc.returncode:
        raise RuntimeError('"{0}" failed with: {1}'.format(command, stderr.decode()))

    if binary:
        return stdout
    else:
        return stdout.decode()


def upload_to_paste(data):
    return execute(command='ya paste', input=data).strip()


def upload_to_mds(data):
    output = execute(command='ya upload --mds --ttl 730 --stdin-tag output.txt --json-output', input=data)
    return json.loads(output)['download_link']


def upload_to_sandbox(data):
    output = execute(command='ya upload --ttl 730 --stdin-tag output.txt --json-output', input=data)
    return json.loads(output)['download_link']
