"""
Utility functions.
"""

import re
import subprocess
from itertools import islice
from typing import Any, Iterable, Iterator, Optional

from click import Context

from ch_tools.common import logging
from ch_tools.common.clickhouse.client.clickhouse_client import clickhouse_client
from ch_tools.monrun_checks.clickhouse_info import ClickhouseInfo


def execute_query(
    ctx: Context,
    query: Any,
    timeout: Optional[int] = None,
    echo: Optional[bool] = False,
    dry_run: Optional[bool] = False,
    format_: Optional[str] = "default",
    stream: bool = False,
    settings: Optional[Any] = None,
    replica: Optional[str] = None,
    **kwargs: Any,
) -> Any:
    """
    Execute ClickHouse query.
    """
    if format_ == "default":
        format_ = "PrettyCompact"

    ch_client = clickhouse_client(ctx)

    if replica is not None:
        ch_client.host = replica

    return ch_client.query(
        query=query,
        query_args=kwargs,
        timeout=timeout,
        format_=format_,
        echo=echo,
        dry_run=dry_run,
        stream=stream,
        settings=settings,
    )


def execute_query_on_shard(
    ctx: Context,
    query: str,
    timeout: Optional[int] = None,
    echo: Optional[bool] = False,
    dry_run: Optional[bool] = False,
    format_: Optional[str] = "default",
    stream: bool = False,
    settings: Optional[Any] = None,
    **kwargs: Any,
) -> None:
    replicas = ClickhouseInfo.get_replicas(ctx)
    for replica in replicas:
        execute_query(
            ctx,
            query,
            timeout=timeout,
            echo=echo,
            dry_run=dry_run,
            format_=format_,
            stream=stream,
            settings=settings,
            replica=replica,
            **kwargs,
        )


def format_query(query: str) -> str:
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


def remove_from_disk(path: str) -> Any:
    cmd = f"rm -rf {path}"
    logging.info("Run : {}", cmd)

    proc = subprocess.run(
        cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    return (proc.returncode, proc.stderr)
