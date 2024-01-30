import logging
import os
import sys
import time

from click import FloatRange, group, option, pass_context
from requests.exceptions import ReadTimeout

from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common.cli.parameters import TimeSpanParamType
from ch_tools.common.commands.replication_lag import estimate_replication_lag
from ch_tools.common.utils import execute

BASE_TIMEOUT = 600
LOCAL_PART_LOAD_SPEED = 10  # in data parts per second
S3_PART_LOAD_SPEED = 0.5  # in data parts per second


@group("wait")
def wait_group():
    """Commands to wait until Clickhouse is in a certain state."""
    pass


@wait_group.command("replication-sync")
@option(
    "--replica-timeout",
    "replica_timeout",
    type=TimeSpanParamType(),
    default="1h",
    help="Timeout for SYNC REPLICA command.",
)
@option(
    "--total-timeout",
    "total_timeout",
    type=TimeSpanParamType(),
    default="3d",
    help="Max amount of time to wait.",
)
@option(
    "-s",
    "--status",
    type=int,
    default=0,
    help="Wait until replication-lag returned status is no worse than given, 0 = OK, 1 = WARN, 2 = CRIT.",
)
@option(
    "-p",
    "--pause",
    type=TimeSpanParamType(),
    default="30s",
    help="Pause between replication lag requests.",
)
@option(
    "-x",
    "--exec-critical",
    "xcrit",
    type=int,
    default=3600,
    help="Critical threshold for one task execution.",
)
@option(
    "-c",
    "--critical",
    "crit",
    type=int,
    default=600,
    help="Critical threshold for lag with errors.",
)
@option("-w", "--warning", "warn", type=int, default=300, help="Warning threshold.")
@option(
    "-M",
    "--merges-critical",
    "mcrit",
    type=FloatRange(0.0, 100.0),
    default=90.0,
    help="Critical threshold in percent of max_replicated_merges_in_queue.",
)
@option(
    "-m",
    "--merges-warning",
    "mwarn",
    type=FloatRange(0.0, 100.0),
    default=50.0,
    help="Warning threshold in percent of max_replicated_merges_in_queue.",
)
@pass_context
def wait_replication_sync_command(
    ctx,
    replica_timeout,
    total_timeout,
    status,
    pause,
    xcrit,
    crit,
    warn,
    mwarn,
    mcrit,
):
    """Wait for ClickHouse server to sync replication with other replicas."""

    start_time = time.time()
    deadline = start_time + total_timeout.total_seconds()

    # Get replicated tables
    tables = execute_query(
        ctx,
        "SELECT name, database FROM system.tables WHERE engine = 'ReplicatedMergeTree'",
        format_="JSON",
    )["data"]

    # Sync tables in cycle
    for t in tables:
        full_name = f"{t['database']}.{t['name']}"
        time_left = deadline - time.time()

        try:
            execute_query(
                ctx,
                f"SYSTEM SYNC REPLICA {full_name}",
                format_=None,
                timeout=min(replica_timeout.total_seconds(), time_left),
            )
        except ReadTimeout:
            print(f"Timeout while runnung SYNC REPLICA on {full_name}.")
            sys.exit(1)
        except:
            raise

    # Replication lag
    while time.time() < deadline:
        res = estimate_replication_lag(ctx, xcrit, crit, warn, mwarn, mcrit)
        if res.code <= status:
            sys.exit(0)
        time.sleep(pause.total_seconds())

    print(f"Timeout while waiting on replication-lag command.")
    sys.exit(1)


@wait_group.command("started")
@option(
    "--timeout",
    type=int,
    help="Max amount of time to wait, in seconds. If not set, the timeout is determined dynamically"
    " based on data part count.",
)
@option("-q", "--quiet", is_flag=True, default=False, help="Quiet mode.")
@pass_context
def wait_started_command(ctx, timeout, quiet):
    """Wait for ClickHouse server to start up."""
    if not quiet:
        logging.basicConfig(level="INFO", format="%(message)s")

    if not timeout:
        timeout = get_timeout()

    deadline = time.time() + timeout

    ch_is_alive = False
    while time.time() < deadline:
        if is_clickhouse_alive():
            ch_is_alive = True
            break
        time.sleep(1)

    if ch_is_alive:
        warmup_system_users(ctx)
        sys.exit(0)

    logging.error("ClickHouse is dead")
    sys.exit(1)


def get_timeout():
    """
    Calculate and return timeout.
    """
    timeout = BASE_TIMEOUT
    timeout += int(get_local_data_part_count() / LOCAL_PART_LOAD_SPEED)
    timeout += int(get_s3_data_part_count() / S3_PART_LOAD_SPEED)
    return timeout


def get_local_data_part_count():
    """
    Return approximate number of data parts stored locally.
    """
    return int(
        execute(
            "find -L /var/lib/clickhouse/data -mindepth 3 -maxdepth 3 -type d | wc -l"
        )
    )


def get_s3_data_part_count():
    """
    Return approximate number of data parts stored in S3.
    """
    s3_metadata_store_path = "/var/lib/clickhouse/disks/object_storage/store"
    if not os.path.exists(s3_metadata_store_path):
        return 0

    return int(
        execute(
            f"find -L {s3_metadata_store_path} -mindepth 3 -maxdepth 3 -type d | wc -l"
        )
    )


def is_clickhouse_alive():
    """
    Check if ClickHouse server is alive or not.
    """
    try:
        os.chdir("/")
        output = execute("timeout 5 sudo -u monitor /usr/bin/ch-monitoring ping")
        if output == "0;OK\n":
            return True

    except Exception as e:
        logging.error("Failed to perform ch_ping check: %s", repr(e))

    return False


def warmup_system_users(ctx):
    execute_query(ctx, "SELECT count() FROM system.users", timeout=300)
