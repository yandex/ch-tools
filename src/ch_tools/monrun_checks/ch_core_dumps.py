import pathlib
import time
from datetime import datetime

import click

from ch_tools.common.result import Result


@click.command("core-dumps")
@click.option(
    "-t",
    "--core-directory",
    "core_directory",
    default="/var/cores/",
    help="Core dump directory.",
)
@click.option(
    "-n",
    "--crit-interval-seconds",
    "crit_seconds",
    type=int,
    default=60 * 10,
    help="Time interval to check in seconds.",
)
def core_dumps_command(core_directory, crit_seconds):
    """
    Check for core dumps.
    """
    status = 0

    core_dir = pathlib.Path(core_directory)
    if core_dir.exists():
        dumps = get_core_dumps(core_dir, crit_seconds)
        if dumps:
            status = 2
        else:
            # look for old dumps
            dumps = get_core_dumps(core_dir)
            if dumps:
                status = 1
        message = ";".join([f"{f} [{dt}]" for f, dt in dumps])
    else:
        status = 1
        message = f"Core dump directory does not exist: {core_dir}"
    return Result(status, message or "OK")


def get_core_dumps(core_dir, interval_seconds=None):
    """
    Get core dumps dumped during the last `interval_seconds`.
    """
    result = []
    for f in core_dir.iterdir():
        if not (f.is_file() and f.owner() == "clickhouse"):
            continue
        ctime = f.stat().st_ctime
        dt = datetime.fromtimestamp(ctime)
        if interval_seconds is None or (ctime > time.time() - interval_seconds):
            result.append((f, dt))

    return result
