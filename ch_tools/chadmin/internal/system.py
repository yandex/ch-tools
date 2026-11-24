import os
import re
import subprocess
from pathlib import Path
from typing import List

from click import Context

from ch_tools.chadmin.internal.utils import clickhouse_client
from ch_tools.common.utils import version_ge

SYSTEMCTL_PATH = "/usr/bin/systemctl"
SYSTEMD_RUNTIME_PATH = Path("/run/systemd/system")


def _systemd_is_running() -> bool:
    return SYSTEMD_RUNTIME_PATH.is_dir()


def _running_clickhouse_server_pids(proc_path: Path = Path("/proc")) -> List[int]:
    """Find ClickHouse server/watchdog processes without relying on PATH."""
    result = []
    for process_path in proc_path.iterdir():
        if not process_path.name.isdecimal():
            continue
        try:
            argv = (process_path / "cmdline").read_bytes().rstrip(b"\0").split(b"\0")
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        if not argv or not argv[0]:
            continue
        executable = os.path.basename(os.fsdecode(argv[0]))
        clickhouse_subcommand = len(argv) > 1 and os.fsdecode(argv[1]) == "server"
        if executable in {"clickhouse-server", "clickhouse-watchdog"} or (
            executable == "clickhouse" and clickhouse_subcommand
        ):
            result.append(int(process_path.name))
    return result


def ensure_clickhouse_server_stopped() -> None:
    """Fail unless the managed service and ClickHouse processes are stopped."""
    if _systemd_is_running():
        state = subprocess.check_output(
            [
                SYSTEMCTL_PATH,
                "show",
                "--property=ActiveState",
                "--value",
                "clickhouse-server.service",
            ],
            text=True,
        ).strip()
        if state not in {"inactive", "failed"}:
            raise RuntimeError(
                "ClickHouse systemd service is not stopped: "
                f"clickhouse-server.service is {state!r}"
            )

    running_pids = _running_clickhouse_server_pids()
    if running_pids:
        raise RuntimeError(
            f"ClickHouse server processes are still running: {running_pids}"
        )


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


def is_yandex_cloud_version(ctx: Context) -> bool:
    """
    Checks if Yandex Cloud build is used.
    """
    pattern = r"^([0-9.]+)-yc\.([0-9.]+)$"
    return re.match(pattern, get_version(ctx)) is not None
