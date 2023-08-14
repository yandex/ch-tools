"""
Formatting module.
"""

import csv
import json
import sys
from collections import OrderedDict
from datetime import date, datetime, timedelta
from decimal import Decimal
from itertools import chain
from typing import Mapping

import humanfriendly
from click import style
from cloup import Color
from deepdiff.helper import notpresent
from pygments import highlight
from pygments.formatters.terminal256 import Terminal256Formatter
from pygments.lexers.data import JsonLexer, YamlLexer
from pygments.style import Style
from pygments.token import Token
from tabulate import tabulate
from termcolor import colored

from .utils import get_timezone
from .yaml import dump_yaml


class FormatStyle(Style):
    styles = {
        Token.Name.Tag: "bold ansibrightblue",
        Token.Punctuation: "bold ansiwhite",
        Token.String: "ansigreen",
    }


def print_header(header):
    print(header)
    print("-" * len(header))


def print_response(
    ctx,
    value,
    format_=None,
    default_format=None,
    field_formatters=None,
    table_formatter=None,
    fields=None,
    ignored_fields=None,
    quiet=None,
    id_key=None,
    separator=None,
    limit=None,
):
    if format_ is None:
        # command-line parameter
        format_ = ctx.obj.get("format")
    if format_ is None:
        # `default_format` function parameter
        format_ = default_format
    if format_ is None:
        # config file parameter
        format_ = ctx.obj.get("config", {}).get("default_format", "json")

    if separator is None:
        separator = ","
    else:
        separator = separator.replace(r"\n", "\n")

    value = _purify_value(
        ctx,
        value,
        formatters=get_formatters(ctx),
        field_formatters=field_formatters,
        include_keys=fields,
        exclude_keys=ignored_fields,
    )

    if limit and isinstance(value, list):
        value = value[:limit]

    if quiet:
        if id_key is None:
            id_key = "id"

        if isinstance(value, list):
            result = separator.join(item[id_key] for item in value)
        else:
            result = value[id_key]

        print(result)
        return

    if format_ in ("table", "csv"):
        if table_formatter:
            value = [table_formatter(v) for v in value]

        if format_ == "table":
            print_table(value)
        else:
            print_csv(value)

    elif format_ == "yaml":
        print_yaml(ctx, value)

    else:
        print_json(ctx, value)


def _purify_value(
    ctx,
    value,
    formatters=None,
    field_formatters=None,
    include_keys=None,
    exclude_keys=None,
):
    for formatter in formatters or []:
        value = formatter(value)

    if isinstance(value, Mapping):
        result = OrderedDict()
        for key in include_keys or value.keys():
            if exclude_keys and key in exclude_keys:
                continue

            item = value[key]

            formatter = field_formatters.get(key) if field_formatters else None
            item = (
                formatter(item)
                if formatter
                else _purify_value(ctx, item, formatters=formatters)
            )

            result[key] = item

        return result

    if isinstance(value, list):
        return [
            _purify_value(
                ctx,
                item,
                formatters=formatters,
                field_formatters=field_formatters,
                include_keys=include_keys,
                exclude_keys=exclude_keys,
            )
            for item in value
        ]

    if isinstance(value, datetime):
        return format_timestamp(ctx, value)

    if isinstance(value, date):
        return format_date(value)

    if isinstance(value, timedelta):
        return str(value)

    if isinstance(value, Decimal):
        return str(value)

    return value


def print_diff(diff, key_separator="."):
    """
    Print structural diff between 2 values.
    """
    if not diff:
        return

    items = chain.from_iterable(diff.values())
    for item in sorted(items, key=lambda i: i.path(output_format="list")):
        _print_diff_item(item, key_separator=key_separator)


def _print_diff_item(item, key_separator):
    item_path = item.path(output_format="list")
    if item_path:
        print("@ " + key_separator.join(str(value) for value in item_path))

    if item.t1 is not notpresent:
        _print_diff_item_value(item.t1, "- ", "red")

    if item.t2 is not notpresent:
        _print_diff_item_value(item.t2, "+ ", "green")


def _print_diff_item_value(value, prefix, color):
    value = json.dumps(value, indent=2, ensure_ascii=False)
    value = "\n".join(f"{prefix}{line}" for line in value.splitlines())
    if sys.stdout.isatty():
        value = colored(value, color=color)
    print(value)


def print_json(ctx, value):
    """
    Print JSON value.
    """
    json_dump = json.dumps(value, indent=2, ensure_ascii=False)
    if _color(ctx):
        print(
            highlight(json_dump, JsonLexer(), Terminal256Formatter(style=FormatStyle)),
            end="",
        )
    else:
        print(json_dump)


def print_yaml(ctx, value):
    """
    Print YAML value.
    """
    yaml_dump = dump_yaml(value)
    if _color(ctx):
        print(
            highlight(yaml_dump, YamlLexer(), Terminal256Formatter(style=FormatStyle)),
            end="",
        )
    else:
        print(yaml_dump)


def print_table(value):
    print(tabulate(value, headers="keys"))


def print_csv(value):
    if value:
        writer = csv.DictWriter(sys.stdout, fieldnames=value[0].keys())
        writer.writeheader()
        writer.writerows(value)


def format_list(value):
    return ",".join(value)


def format_bytes(value):
    if value is None:
        return None

    if isinstance(value, str):
        value = int(value)

    if value > 0:
        return humanfriendly.format_size(value, binary=True)

    if value < 0:
        return "-{0}".format(humanfriendly.format_size(-value, binary=True))

    return "0"


def format_bytes_per_second(value):
    if value is None:
        return None

    if value == 0:
        return "0"

    return f"{format_bytes(value)}/s"


def format_date(value):
    """
    Format date value.
    """
    return value.strftime("%Y-%m-%d")


def format_timestamp(ctx, value):
    """
    Format timestamp value.
    """
    value = value.astimezone(get_timezone(ctx))
    result = value.strftime("%Y-%m-%d %H:%M:%S")
    result += f".{int(value.microsecond / 1000):03d}"
    return result


def format_duration(value):
    return humanfriendly.format_timespan(value)


def format_percents(value):
    return f"{round(100 * value, 2)} %"


def format_float(value):
    """
    Format float value.
    """
    return round(value, 3)


def register_formatter(ctx, formatter):
    """
    Register output formatter.
    """
    if "formatters" not in ctx.obj:
        ctx.obj["formatters"] = []

    ctx.obj["formatters"].append(formatter)


def get_formatters(ctx):
    """
    Return list of registered output formatters.
    """
    return ctx.obj.get("formatters", [])


def _color(ctx):
    """
    Return True if output should be colored, or False otherwise.
    """
    color = ctx.obj.get("color")
    if color is not None:
        return color

    return sys.stdout.isatty()


def format_var(var_name: str) -> str:
    return style(var_name, fg=Color.cyan)


def format_code(code: str, padding: bool = True) -> str:
    return style(
        f" {code} " if padding else code,
        fg=Color.bright_green,
        bg=Color.bright_black,
        bold=True,
    )


def format_db_name(name: str) -> str:
    return style(name, italic=True, fg=Color.bright_cyan)


def format_table_name(name: str) -> str:
    return format_db_name(name)


def format_product_name(name: str) -> str:
    return style(name, bold=True, fg=Color.bright_blue)


def format_metavar(var: str) -> str:
    return style(var, fg=Color.bright_yellow)


def format_path(path: str) -> str:
    return style(path, italic=True)
