import re
from datetime import datetime, timedelta

import click
from file_read_backwards import FileReadBackwards

from ch_tools.common.cli.parameters import RegexpParamType
from ch_tools.common.result import CRIT, OK, WARNING, Result

REGEXP = re.compile(
    r"^([0-9]{4}\.[0-9]{2}\.[0-9]{2}\ [0-9]{2}\:[0-9]{2}\:[0-9]{2}).*?<(Error|Fatal)>"
)


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
    type=RegexpParamType(),
    help="Excluded error.",
)
@click.option(
    "-f",
    "--logfile",
    "logfile",
    help="Log file path.",
)
def log_errors_command(crit, warn, watch_seconds, exclude, logfile):
    """
    Check errors in ClickHouse server logs.
    """
    datetime_start = datetime.now() - timedelta(seconds=watch_seconds)
    errors = 0

    with FileReadBackwards(logfile, encoding="utf-8") as f:
        for line in f:
            if exclude.search(line):
                continue
            match = REGEXP.match(line)
            if match is None:
                continue
            date = match.group(1)
            if datetime.strptime(date, "%Y.%m.%d %H:%M:%S") < datetime_start:
                break
            errors += 1

    msg = f"{errors} errors for last {watch_seconds} seconds"
    if errors >= crit:
        return Result(CRIT, msg)
    if errors >= warn:
        return Result(WARNING, msg)
    return Result(OK, f"OK, {msg}")
