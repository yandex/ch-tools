from typing import Any

import click

from ch_tools.common.result import CRIT, OK, WARNING, Result
from ch_tools.common.utils import count_log_messages

ERROR_MESSAGE_PATTERN = "<(Error|Fatal)>"


@click.command("log-errors")
@click.option("-c", "--critical", "crit", type=int, help="Critical threshold.")
@click.option("-w", "--warning", "warn", type=int, help="Warning threshold.")
@click.option(
    "-n",
    "--watch-seconds",
    "watch_seconds",
    type=int,
    help="Watch seconds.",
)
@click.option(
    "-e",
    "--exclude",
    "exclude",
    type=str,
    help="Excluded error.",
)
@click.option(
    "-f",
    "--logfile",
    "logfile",
    help="Log file path.",
)
def log_errors_command(
    crit: int, warn: int, watch_seconds: int, exclude: Any, logfile: str
) -> Result:
    """
    Check errors in ClickHouse server logs.
    """
    error_occurrences = count_log_messages(
        logfile, watch_seconds, ERROR_MESSAGE_PATTERN, exclude
    )

    msg = f"{error_occurrences} errors for last {watch_seconds} seconds"
    if error_occurrences >= crit:
        return Result(CRIT, msg)
    if error_occurrences >= warn:
        return Result(WARNING, msg)
    return Result(OK, f"OK, {msg}")
