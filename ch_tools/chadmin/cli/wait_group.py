import os
import sys
import time

import requests
from click import FloatRange, group, option, pass_context

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.clickhouse_disks import S3_METADATA_STORE_PATH
from ch_tools.chadmin.internal.table_replica import list_table_replicas
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.cli.parameters import TimeSpanParamType
from ch_tools.common.clickhouse.client.error import ClickhouseError
from ch_tools.common.commands.replication_lag import estimate_replication_lag
from ch_tools.common.utils import execute

BASE_TIMEOUT = 600
LOCAL_PART_LOAD_SPEED = 10  # in data parts per second
S3_PART_LOAD_SPEED = 0.5  # in data parts per second


@group("wait", cls=Chadmin)
def wait_group():
    """Commands to wait until Clickhouse is in a certain state."""
    pass


@wait_group.command("replication-sync")
@option(
    "--replica-timeout",
    "replica_timeout",
    type=TimeSpanParamType(),
    help="Timeout for SYNC REPLICA command.",
)
@option(
    "--total-timeout",
    "total_timeout",
    type=TimeSpanParamType(),
    help="Max amount of time to wait.",
)
@option(
    "-s",
    "--status",
    type=int,
    help="Wait until replication-lag returned status is no worse than given, 0 = OK, 1 = WARN, 2 = CRIT.",
)
@option(
    "-p",
    "--pause",
    type=TimeSpanParamType(),
    help="Pause between replication lag requests.",
)
@option(
    "-x",
    "--exec-critical",
    "xcrit",
    type=int,
    help="Critical threshold for one task execution.",
)
@option(
    "-c",
    "--critical",
    "crit",
    type=int,
    help="Critical threshold for lag with errors.",
)
@option("-w", "--warning", "warn", type=int, help="Warning threshold.")
@option(
    "-M",
    "--merges-critical",
    "mcrit",
    type=FloatRange(0.0, 100.0),
    help="Critical threshold in percent of max_replicated_merges_in_queue.",
)
@option(
    "-m",
    "--merges-warning",
    "mwarn",
    type=FloatRange(0.0, 100.0),
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

    try:
        # Sync tables in cycle
        for replica in list_table_replicas(ctx):
            full_name = f"`{replica['database']}`.`{replica['table']}`"
            time_left = deadline - time.time()

            execute_query(
                ctx,
                f"SYSTEM SYNC REPLICA {full_name}",
                format_=None,
                timeout=replica_timeout.total_seconds(),
                settings={"receive_timeout": time_left},
            )
    except requests.exceptions.ReadTimeout:
        logging.error("Read timeout while running query.")
        sys.exit(1)
    except requests.exceptions.ConnectionError:
        logging.error("Connection error while running query.")
        sys.exit(1)
    except ClickhouseError as e:
        if "TIMEOUT_EXCEEDED" in str(e):
            logging.error("Timeout while running query.")
            sys.exit(1)
        raise

    # Replication lag
    while time.time() < deadline:
        res = estimate_replication_lag(ctx, xcrit, crit, warn, mwarn, mcrit)
        if res.code <= status:
            sys.exit(0)
        time.sleep(pause.total_seconds())

    logging.error("Timeout while waiting on replication-lag command.")
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
    if quiet:
        logging.disable_stdout_logger()
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
    if not os.path.exists(S3_METADATA_STORE_PATH):
        return 0

    return int(
        execute(
            f"find -L {S3_METADATA_STORE_PATH} -mindepth 3 -maxdepth 3 -type d | wc -l"
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
        logging.error("Failed to perform ch_ping check: {!r}", e)

    return False


def warmup_system_users(ctx):
    execute_query(ctx, "SELECT count() FROM system.users", timeout=300)
