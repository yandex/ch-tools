import re
from datetime import datetime, timedelta

import click
from file_read_backwards import FileReadBackwards

from ch_tools.common.result import Result

REGEXP = re.compile(
    r"^([0-9]{4}\.[0-9]{2}\.[0-9]{2}\ [0-9]{2}\:[0-9]{2}\:[0-9]{2}).*?<(Error|Fatal)>"
)
DEFAULT_EXCLUDE = r"e\.displayText\(\) = No message received"


def validate_exclude(ctx, param, value):  # pylint: disable=unused-argument
    try:
        return re.compile(value if value is not None else DEFAULT_EXCLUDE)
    except re.error:
        raise click.BadParameter("Value should be a valid regular expression.")


@click.command("log-errors")
@click.option(
    "-c", "--critical", "crit", type=int, default=60, help="Critical threshold."
)
@click.option("-w", "--warning", "warn", type=int, default=6, help="Warning threshold.")
@click.option(
    "-n",
    "--watch-seconds",
    "watch_seconds",
    type=int,
    default=600,
    help="Watch seconds.",
)
@click.option(
    "-e",
    "--exclude",
    "exclude",
    default=DEFAULT_EXCLUDE,
    callback=validate_exclude,
    help="Excluded error.",
)
@click.option(
    "-f",
    "--logfile",
    "logfile",
    default="/var/log/clickhouse-server/clickhouse-server.err.log",
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
        return Result(2, msg)
    if errors >= warn:
        return Result(1, msg)
    return Result(0, "OK, " + msg)
