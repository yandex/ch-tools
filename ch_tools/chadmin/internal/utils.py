"""
Utility functions.
"""
import re
from itertools import islice
from typing import Iterable, Iterator

from ch_tools.common.clickhouse.client.clickhouse_client import clickhouse_client


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

    while True:
        chunk = list(islice(it, n))
        if not chunk:
            break
        yield chunk
