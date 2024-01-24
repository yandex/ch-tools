import logging
import time
import sys

from click import command, option, pass_context

from ch_tools.common.replication_lag import estimate_replication_lag
from ch_tools.common.cli.parameters import TimeSpanParamType


@command("wait-replication-sync")
@option(
    "-s",
    "--status",
    type=int,
    default=0,
    help="Wait until returned status is no worse than given, 0 = OK, 1 = WARN, 2 = CRIT.",
)
@option(
    "-p",
    "--pause",
    type=TimeSpanParamType(),
    default="30s",
    help="Pause between requests.",
)
@option(
    "-t",
    "--timeout",
    type=TimeSpanParamType(),
    default="3d",
    help="Max amount of time to wait.",
)
@pass_context
def wait_replication_sync_command(ctx, status, pause, timeout):
    """Wait for ClickHouse server to sync replication with other replicas."""

    deadline = time.time() + timeout.total_seconds()
    while time.time() < deadline:
        res = estimate_replication_lag(ctx)
        if res.code <= status:
            sys.exit(0)
        time.sleep(pause.total_seconds())

    logging.error(f"ClickHouse can't sync replica.")
    sys.exit(1)
