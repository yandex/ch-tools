"""
Utility functions.
"""
import re
from itertools import islice
from typing import Iterable, Iterator

from ch_tools.common.clickhouse.client import ClickhouseClient


def clickhouse_client(ctx):
    """
    Return ClickHouse client from the context if it exists.
    Init ClickHouse client and store to the context if it doesn't exist.
    Raise RuntimeError if ClickHouse client's config doesn't exist
    """
    chcli_conf = ctx.obj.get("chcli_conf")
    if not chcli_conf:
        raise RuntimeError(
            "Could not init ClickHouse's connection because there is no chcli config in context. Seems like bug in chadmin."
        )

    if not ctx.obj.get("chcli"):
        ctx.obj["chcli"] = ClickhouseClient(
            port=chcli_conf["port"],
            timeout=chcli_conf["timeout"],
            settings=chcli_conf["settings"],
        )

    return ctx.obj["chcli"]


def execute_query(
    ctx, query, timeout=None, echo=False, dry_run=False, format_="default", **kwargs
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

    chunk = list(islice(it, n))
    while chunk:
        yield chunk
