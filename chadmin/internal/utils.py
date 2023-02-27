"""
Utility functions.
"""
import contextlib
import re

from pathlib import Path
from typing import IO, Iterator


def clickhouse_client(ctx):
    """
    Return ClickHouse client from the context.
    """
    return ctx.obj['chcli']


def execute_query(ctx, query, timeout=None, echo=False, dry_run=False, format='default', **kwargs):
    """
    Execute ClickHouse query.
    """
    if format == 'default':
        format = 'PrettyCompact'

    return clickhouse_client(ctx).query(
        query=query, query_args=kwargs, timeout=timeout, format=format, echo=echo, dry_run=dry_run
    )


def format_query(query):
    """
    Format SQL query for output.
    """
    return re.sub(r'(\A|\n)\s*\n', r'\1', query, re.MULTILINE)


def human_readable_size(size: float, decimal_places: int = 2) -> str:
    """
    Format the size in bytes into a human readable form.

    >>> human_readable_size(1048576)
    1.00 MiB
    """
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']:
        if size < 1024.0 or unit == 'PiB':
            break
        size /= 1024.0
    return f'{size:.{decimal_places}f} {unit}'


@contextlib.contextmanager
def open_if_not_none(*file_paths: Path | None, mode: str) -> Iterator[list[IO | None]]:
    with contextlib.ExitStack() as stack:
        yield [
            (stack.enter_context(file_path.open(mode)) if file_path is not None else None) for file_path in file_paths
        ]
