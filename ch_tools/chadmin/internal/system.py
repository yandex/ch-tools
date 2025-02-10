import re

from click import Context
from pkg_resources import parse_version

from ch_tools.chadmin.internal.utils import clickhouse_client


def validate_version(version: str) -> None:
    pattern = r"^\d+\.\d+\.\d+\.\d+$"

    assert re.match(pattern, version), f"version={version} has broken format"


def get_version(ctx: Context) -> str:
    """
    Get ClickHouse version.
    """

    ch_version_from_config = ctx.obj["config"]["clickhouse"]["version"]
    if ch_version_from_config:
        return ch_version_from_config
    return clickhouse_client(ctx).get_clickhouse_version()


def match_ch_version(ctx: Context, min_version: str) -> bool:
    """
    Returns True if ClickHouse version >= min_version.
    """
    return match_str_ch_version(get_version(ctx), min_version)


def match_str_ch_version(version: str, min_version: str) -> bool:
    """
    Returns True if ClickHouse version >= min_version.
    """
    validate_version(version)
    return parse_version(version) >= parse_version(min_version)
