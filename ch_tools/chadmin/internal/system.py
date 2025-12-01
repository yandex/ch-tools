import subprocess

from click import Context

from ch_tools.chadmin.internal.utils import clickhouse_client
from ch_tools.common.utils import version_ge


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
    return version_ge(get_version(ctx), min_version)


def match_ch_backup_version(min_version: str) -> bool:
    """
    Returns True if ClickHouse version >= min_version.
    """
    cmd = ["ch-backup", "version"]
    proc = subprocess.run(
        cmd,
        shell=False,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if proc.returncode:
        raise RuntimeError(
            f"Failed to get ch-backup version: retcode {proc.returncode}, stderr: {proc.stderr.decode()}"
        )

    return version_ge(proc.stdout.decode(), min_version)
