"""
Utility functions.
"""

import os
import re
import shutil
from itertools import islice
from typing import Any, Iterable, Iterator, Optional

from click import Context

from ch_tools.common import logging
from ch_tools.common.clickhouse.client.clickhouse_client import clickhouse_client
from ch_tools.common.clickhouse.config.clickhouse import ClickhousePort
from ch_tools.monrun_checks.clickhouse_info import ClickhouseInfo

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


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

    return ch_client.query(
        query=query,
        query_args=kwargs,
        timeout=timeout,
        format_=format_,
        echo=echo,
        dry_run=dry_run,
        stream=stream,
        settings=settings,
        host=replica,
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


def get_remote_table_for_hosts(ctx: Context, table: str, replicas: list[str]) -> str:
    """
    Get remote/remoteSecure function for a table with given remote replicas.
    """
    ch_client = clickhouse_client(ctx)
    user_name = ch_client.user or ""

    #  It is believed that all hosts in shard have the same port set, so check current for tcp port
    if ch_client.check_port(ClickhousePort.TCP_SECURE):
        remote_clause = "remoteSecure"
    elif ch_client.check_port(ClickhousePort.TCP):
        remote_clause = "remote"
    else:
        raise RuntimeError("For using remote() table function tcp port must be defined")

    replicas_str = ",".join(replicas)
    return f"{remote_clause}('{replicas_str}', {table}, '{user_name}', '{{user_password}}')"


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


def remove_from_disk(path: str) -> None:
    """
    Remove file or directory with its all content.

    Behaviour is similar to 'rm -rf'.

    Args:
        path: Path to file or directory to remove
    Raises:
        OSError: If removal fails due to permissions or other system errors
    """
    logging.info(f"Removing path: {path}")

    if os.path.isfile(path) or os.path.islink(path):
        os.remove(path)
    elif os.path.isdir(path):
        shutil.rmtree(path)
    else:
        # Path doesn't exist, which is equivalent to successful rm -rf
        pass
