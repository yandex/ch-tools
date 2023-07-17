from click import Context
from pkg_resources import parse_version

from ch_tools.chadmin.internal.utils import clickhouse_client


def get_version(ctx: Context) -> str:
    """
    Get ClickHouse version.
    """
    return clickhouse_client(ctx).get_clickhouse_version()


def match_ch_version(ctx: Context, min_version: str) -> bool:
    """
    Returns True if ClickHouse version >= min_version.
    """
    return parse_version(get_version(ctx)) >= parse_version(min_version)
