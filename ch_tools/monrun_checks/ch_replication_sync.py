import time

from click import command, option, pass_context

from ch_tools.common.result import Result
from ch_tools.monrun_checks.ch_replication_lag import estimate_replication_lag


@command("wait-replication-sync")
@option(
    "-s",
    "--status",
    type=int,
    default=0,
    help="Wait until returned status is no worse than given, 0 = OK (default), 1 = WARN, 2 = CRIT.",
)
@option(
    "-p",
    "--pause",
    type=int,
    default=30,
    help="Pause between request in seconds, default is 30 seconds.",
)
@option(
    "-t",
    "--timeout",
    type=int,
    default=3 * 24 * 60 * 60,
    help="Max amount of time to wait, in seconds. Default is 30 days.",
)
@pass_context
def wait_replication_sync_command(ctx, status, pause, timeout):
    """Wait for ClickHouse server to sync replication with other replicas."""

    deadline = time.time() + timeout
    while time.time() < deadline:
        res = estimate_replication_lag(ctx)
        if res.code <= status:
            return Result(code=0) 
        time.sleep(pause)

    return Result(code=2, message=f"ClickHouse can\'t sync replica for {timeout} seconds")
