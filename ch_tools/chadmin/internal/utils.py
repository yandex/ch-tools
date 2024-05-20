"""
Utility functions.
"""
import re
import subprocess
from itertools import islice
from typing import Iterable, Iterator

from ch_tools.common import logging
from ch_tools.common.clickhouse.client.clickhouse_client import clickhouse_client


def execute_query(
    ctx,
    query,
    timeout=None,
    echo=False,
    dry_run=False,
    format_="default",
    stream=False,
    settings=None,
    **kwargs,
):
    """
    Execute ClickHouse query.
    """
    if format_ == "default":
        format_ = "PrettyCompact"

    return clickhouse_client(ctx).query(
        query=query,
        query_args=kwargs,
        timeout=timeout,
        format_=format_,
        echo=echo,
        dry_run=dry_run,
        stream=stream,
        settings=settings,
    )


def format_query(query):
    """
    Format SQL query for output.
    """
    return re.sub(r"(\A|\n)\s*\n", r"\1", query, re.MULTILINE)


def chunked(iterable: Iterable, n: int) -> Iterator[list]:
    """
    Chunkify data into lists of length n. The last chunk may be shorter.

    Based on https://docs.python.org/3/library/itertools.html#itertools-recipes

    >>> chunked('ABCDEFG', 3)
    ABC DEF G
    """
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)

    while True:
        chunk = list(islice(it, n))
        if not chunk:
            break
        yield chunk


def replace_macros(string: str, macros: dict) -> str:
    """
    Substitute macros in the specified string. Macros in string are specified in the form "{macro_name}".

    Example:
    >>> replace_macros('{a} and {b}', {'a': '1', 'b': '2'})
    1 and 2
    """
    return re.sub(
        string=string,
        pattern=r"{([^{}]+)}",
        repl=lambda m: macros.get(m.group(1), m.group(0)),
    )


def remove_from_disk(path):
    cmd = f"rm -rf {path}"
    logging.info("Run : {}", cmd)

    proc = subprocess.run(
        cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    return (proc.returncode, proc.stderr)
