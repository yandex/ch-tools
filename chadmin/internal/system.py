from cloud.mdb.clickhouse.tools.chadmin.internal.utils import clickhouse_client
from pkg_resources import parse_version


def get_version(ctx):
    """
    Get ClickHouse version.
    """
    return clickhouse_client(ctx).get_clickhouse_version()


def match_ch_version(ctx, min_version: str) -> bool:
    """
    Returns True if ClickHouse version >= min_version.
    """
    return parse_version(get_version(ctx)) >= parse_version(min_version)
