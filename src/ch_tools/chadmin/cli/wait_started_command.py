import logging
import os
import sys
import time

from click import command, option, pass_context

from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common.utils import execute

BASE_TIMEOUT = 600
LOCAL_PART_LOAD_SPEED = 10  # in data parts per second
S3_PART_LOAD_SPEED = 0.5  # in data parts per second


@command("wait-started")
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
