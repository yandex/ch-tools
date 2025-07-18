"""
Command-line parameters.
"""

import os
import re
import sys
from typing import Any, Sequence, Union

import click
import humanfriendly
import yaml
from click import ClickException
from yaml.error import YAMLError

from .formatting import format_var
from .utils import parse_timespan


class ListParamType(click.ParamType):
    """
    Command-line parameter type for lists. It supports reading from file and stdin.
    """

    # pylint: disable=redefined-builtin

    name = "list"

    def __init__(self, type: Any = None, separator: str = r"[,\s]+"):  # noqa: A002
        self.type = type
        self.separator = separator

    def convert(self, value, param, ctx):  # type: ignore
        value = _preprocess_value(value)
        result = [v.strip() for v in re.split(self.separator, value) if v]

        if self.type:
            if isinstance(self.type, click.ParamType):
                result = [self.type(v, param=param, ctx=ctx) for v in result]
            else:
                result = [self.type(v) for v in result]

        return result


class YamlParamType(click.ParamType):
    """
    Command-line parameter type for YAML values. It supports reading from file and stdin.
    """

    name = "yaml"

    def convert(self, value, param, ctx):  # type: ignore
        try:
            value = _preprocess_value(value)
            return yaml.safe_load(value)
        except YAMLError as e:
            raise ClickException(
                f'Invalid YAML value for the parameter "{param.name}". {str(e)}'
            )


class StringParamType(click.ParamType):
    """
    Command-line parameter type for string values. It supports reading from file and stdin.
    """

    name = "string"

    def convert(self, value, param, ctx):  # type: ignore
        return _preprocess_value(value)


class RegexpParamType(click.ParamType):
    """
    Command-line parameter type for regular expressions.
    """

    name = "regexp"

    def convert(self, value, param, ctx):  # type: ignore
        try:
            return re.compile(value)
        except re.error:
            raise click.BadParameter("Value should be a valid regular expression.")


class TimeSpanParamType(click.ParamType):
    """
    Command-line parameter type for times span values.
    """

    name = "timespan"

    def convert(self, value, param, ctx):  # type: ignore
        try:
            return parse_timespan(value)
        except humanfriendly.InvalidTimespan as e:
            raise ClickException(
                f'Invalid timespan value for the parameter "{param.name}": {str(e)}'
            )


class BytesParamType(click.ParamType):
    """
    Command-line parameter type for bytes values.
    """

    name = "bytes"

    def convert(self, value, param, ctx):  # type: ignore
        if isinstance(value, str):
            value = value.strip()
            if value.startswith("-"):
                value = value[1:]
                multiplier = -1
            else:
                multiplier = 1

            return multiplier * humanfriendly.parse_size(value, binary=True)

        return value


def _preprocess_value(value: Any) -> str:
    """
    Preprocess command-line parameter value. It adds support of reading from file and stdin.
    """
    if value == "-":
        return sys.stdin.read()

    if value.startswith("@"):
        with open(os.path.expanduser(value[1:]), encoding="utf-8") as f:
            return f.read()

    return str(value)


def env_var_help(v: Union[str, Sequence[str]]) -> str:
    """
    Returns help message declaring how parameter could be set via environment variables.
    """
    var_names: Sequence[str] = [v] if isinstance(v, str) else v
    return f'Could be set via ENV var{"s" if len(var_names) > 1 else ""} {", ".join(map(format_var, var_names))}.'
