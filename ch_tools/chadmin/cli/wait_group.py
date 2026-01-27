import os
import time
from datetime import timedelta
from typing import Any, Optional

import psutil
import requests
from cloup import (
    Choice,
    Context,
    FloatRange,
    group,
    option,
    pass_context,
)
from cloup.constraints import (
    AcceptAtMost,
    Equal,
    If,
    accept_none,
    constraint,
)
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    retry_if_not_exception_message,
    stop_after_attempt,
    stop_after_delay,
    wait_random_exponential,
)

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.clickhouse_disks import S3_METADATA_STORE_PATH
from ch_tools.chadmin.internal.database import list_databases
from ch_tools.chadmin.internal.system import match_ch_version
from ch_tools.chadmin.internal.table_replica import list_table_replicas
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.cli.parameters import TimeSpanParamType
from ch_tools.common.clickhouse.client.error import ClickhouseError
from ch_tools.common.commands.replication_lag import estimate_replication_lag
from ch_tools.common.result import Result
from ch_tools.common.utils import execute

BASE_TIMEOUT = 600
LOCAL_PART_LOAD_SPEED = 10  # in data parts per second
S3_PART_LOAD_SPEED = 0.5  # in data parts per second
CLICKHOUSE_TIMEOUT_EXCEEDED_MSG = "TIMEOUT_EXCEEDED"
PID_MAX_CHECK_ATTEMPTS = 90
DEFAULT_FILE_PROCESSING_SPEED = 100  # files per second
DEFAULT_MIN_TIMEOUT = 2 * 60 * 60  # in seconds
DEFAULT_MAX_TIMEOUT = 6 * 60 * 60  # in seconds


@group("wait", cls=Chadmin)
def wait_group() -> None:
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
    "--sync-databases/--no-sync-databases",
    "sync_databases",
    is_flag=True,
    default=True,
    help="Sync replicated database replicas",
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
@option(
    "--lightweight/--full",
    is_flag=True,
    default=True,
    help="Use SYNC REPLICA with LIGHTWEIGHT option and skip replication lag check.",
)
@option(
    "--sync-query-max-retries",
    "sync_query_max_retries",
    type=int,
    default=10,
    help="Max number of retries for system sync replica queries.",
)
@option(
    "--sync-query-max-backoff",
    "sync_query_max_backoff",
    type=int,
    default=10,
    help="Max backoff interval for sync query retry in seconds.",
)
@pass_context
def wait_replication_sync_command(
    ctx: Context,
    replica_timeout: Any,
    total_timeout: Any,
    status: int,
    pause: Any,
    xcrit: int,
    crit: int,
    warn: int,
    mwarn: float,
    mcrit: float,
    lightweight: bool,
    sync_query_max_retries: int,
    sync_query_max_backoff: int,
    sync_databases: bool,
) -> None:
    """Wait for ClickHouse server to sync replication with other replicas."""
    # Lightweight sync is added in 23.4
    try:
        if lightweight and not match_ch_version(ctx, "23.4"):
            logging.warning(
                "Lightweight sync requires version 23.4, will do full sync instead."
            )
            lightweight = False
    except Exception as e:
        raise ConnectionError(f"Connection error while getting CH version: {e}")

    start_time = time.time()
    deadline = start_time + total_timeout.total_seconds()

    try:
        # Sync replicated databases
        if sync_databases:
            for database in list_databases(ctx, engine_pattern="Replicated"):
                query = f"SYSTEM SYNC DATABASE REPLICA `{database['database']}`"
                sync_replica_with_retries(
                    ctx,
                    query,
                    replica_timeout,
                    deadline,
                    sync_query_max_retries,
                    sync_query_max_backoff,
                )

        # Sync table replicas
        for replica in list_table_replicas(ctx):
            full_name = f"`{replica['database']}`.`{replica['table']}`"
            query = f"SYSTEM SYNC REPLICA {full_name}"
            if lightweight:
                query = f"{query} LIGHTWEIGHT"
            sync_replica_with_retries(
                ctx,
                query,
                replica_timeout,
                deadline,
                sync_query_max_retries,
                sync_query_max_backoff,
            )

    except requests.exceptions.ReadTimeout:
        raise ConnectionError("Read timeout while running query.")
    except requests.exceptions.ConnectionError:
        raise ConnectionError("Connection error while running query.")
    except ClickhouseError as e:

        if CLICKHOUSE_TIMEOUT_EXCEEDED_MSG in str(e):
            error_msg = "Timeout while running query."
        else:
            error_msg = f"Clickhouse error while running query: {e}"
        raise RuntimeError(error_msg)

    if lightweight:
        return

    # Replication lag
    while time.time() < deadline:
        res: Result = estimate_replication_lag(ctx, xcrit, crit, warn, mwarn, mcrit)
        if res.code <= status:
            return
        time.sleep(pause.total_seconds())
    raise RuntimeError("Timeout while waiting on replication-lag command.")


@wait_group.command("started", show_constraints=True)
@option("-q", "--quiet", "quiet", is_flag=True, default=False, help="Quiet mode.")
@option(
    "--wait-failed-dictionaries",
    "wait_failed_dictionaries",
    is_flag=True,
    default=False,
    help="Should we wait for dictionaries in FAILED or FAILED_AND_RELOADING status.",
)
@option(
    "--track-pid-file",
    "track_pid_file",
    type=str,
    default="",
    help="Path to PID file. Exit if pid from pidfile not running.",
)
@option(
    "--timeout-strategy",
    "timeout_strategy",
    type=Choice(["files", "parts"]),
    default="files",
    help="Strategy to calculate timeout.",
)
@option(
    "--file-processing-speed",
    "file_processing_speed",
    type=int,
    help="Number of files, which expected to be processed in one second.",
)
@option(
    "--min-timeout",
    "min_timeout",
    type=int,
    help="Minimal amount of time to wait, in seconds.",
)
@option(
    "--max-timeout",
    "max_timeout",
    type=int,
    help="Maximal amount of time to wait, in seconds.",
)
@option(
    "-w",
    "--wait",
    "--timeout",
    "wait",
    type=int,
    help="Time to wait, in seconds. If not set, the timeout is determined dynamically based on chosen timeout strategy.",
)
@constraint(
    If(Equal("timeout_strategy", "parts"), then=AcceptAtMost(1), else_=accept_none),
    ["wait"],
)
@constraint(
    If(Equal("timeout_strategy", "files"), then=AcceptAtMost(3), else_=accept_none),
    ["file_processing_speed", "min_timeout", "max_timeout"],
)
@pass_context
def wait_started_command(
    ctx: Context,
    quiet: bool,
    wait_failed_dictionaries: bool,
    track_pid_file: str,
    timeout_strategy: str,
    file_processing_speed: Optional[int],
    min_timeout: Optional[int],
    max_timeout: Optional[int],
    wait: Optional[int],
) -> None:
    """Wait for ClickHouse server to start up."""
    if quiet:
        logging.disable_stdout_logger()

    timeout: int
    if wait:
        timeout = wait
    elif timeout_strategy == "files":
        timeout = get_timeout_by_files(file_processing_speed, min_timeout, max_timeout)
    elif timeout_strategy == "parts":
        timeout = get_timeout_by_parts()

    deadline = time.time() + timeout

    ch_is_alive = False
    pid_check_attempts = 0
    while time.time() < deadline:
        pid_check_attempts += 1

        if is_clickhouse_alive():
            ch_is_alive = True
            break

        time.sleep(1)
        exit_if_pid_not_running(track_pid_file, pid_check_attempts)

    if not ch_is_alive:
        raise ConnectionError("ClickHouse is dead")

    warmup_system_users(ctx)

    pid_check_attempts = 0
    while time.time() < deadline:
        pid_check_attempts += 1
        if is_initial_dictionaries_load_completed(ctx, wait_failed_dictionaries):
            return
        time.sleep(1)
        exit_if_pid_not_running(track_pid_file, pid_check_attempts)


def get_timeout_by_parts() -> int:
    """
    Calculate and return timeout by parts.
    """
    timeout = BASE_TIMEOUT
    timeout += int(get_local_data_part_count() / LOCAL_PART_LOAD_SPEED)
    timeout += int(get_s3_data_part_count() / S3_PART_LOAD_SPEED)
    return timeout


def get_timeout_by_files(
    file_processing_speed: Optional[int],
    min_timeout: Optional[int],
    max_timeout: Optional[int],
) -> int:
    """
    Calculate and return timeout by files.
    """
    file_processing_speed = file_processing_speed or DEFAULT_FILE_PROCESSING_SPEED
    min_timeout = min_timeout or DEFAULT_MIN_TIMEOUT
    max_timeout = max_timeout or DEFAULT_MAX_TIMEOUT

    file_processing_timeout = get_file_count() // file_processing_speed
    return max(min_timeout, min(file_processing_timeout, max_timeout))


def get_file_count() -> int:
    """
    Return number of files stored on data disk.
    """
    return int(execute("df --output=iused /var/lib/clickhouse/ | tail -1"))


def get_local_data_part_count() -> int:
    """
    Return approximate number of data parts stored locally.
    """
    return int(
        execute(
            "find -L /var/lib/clickhouse/data -mindepth 3 -maxdepth 3 -type d | wc -l"
        )
    )


def get_s3_data_part_count() -> int:
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


def is_clickhouse_alive() -> bool:
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


def warmup_system_users(ctx: Context) -> None:
    execute_query(ctx, "SELECT count() FROM system.users", timeout=300)


def is_initial_dictionaries_load_completed(
    ctx: Context, wait_failed_dictionaries: bool
) -> bool:
    """
    Check that initial load of ClickHouse dictionaries completed.
    """
    try:
        if wait_failed_dictionaries:
            query = (
                "SELECT count() FROM system.dictionaries WHERE status IN "
                "('LOADING','FAILED','LOADED_AND_RELOADING','FAILED_AND_RELOADING')"
            )
        else:
            query = "SELECT count() FROM system.dictionaries WHERE status IN ('LOADING', 'LOADED_AND_RELOADING')"
        output = execute_query(ctx, query, timeout=300, format_="TabSeparated")
        if output == "0":
            return True

    except Exception as e:
        logging.error("Failed to get status of ClickHouse dictionaries: {!r}", e)

    return False


def is_pid_file_valid(pid_file_to_check: str) -> bool:
    """
    Verify that PID file exists and the process is running.
    """
    if not pid_file_to_check:
        return True

    try:
        with open(pid_file_to_check, "r", encoding="utf-8") as f:
            pid = int(f.read())
    except FileNotFoundError:
        logging.debug("ClickHouse pid file ({}) not found.", pid_file_to_check)
        return False

    if not psutil.pid_exists(pid):
        logging.debug("ClickHouse pid ({}) not running.", pid)
        return False

    return True


def exit_if_pid_not_running(track_pid_file: str, pid_check_attempts: int) -> None:
    """
    Check PID file and raise exception if process is not running.
    """
    is_valid = is_pid_file_valid(track_pid_file)
    if is_valid or pid_check_attempts < PID_MAX_CHECK_ATTEMPTS:
        return

    raise RuntimeError(
        f'ClickHouse pid file creation out of max tries "{PID_MAX_CHECK_ATTEMPTS}"'
    )


def sync_replica_with_retries(
    ctx: Context,
    query: str,
    replica_timeout: timedelta,
    deadline: float,
    max_retries: int,
    max_backoff: int,
) -> None:
    """
    Sync table or database replica with retries up to deadline.
    """
    time_left = max(deadline - time.time(), 1)

    # 'SYSTEM SYNC REPLICA' and 'SYSTEM SYNC DATABASE REPLICA'
    #  timeout is configured via the 'receive_timeout' setting.
    settings = {"receive_timeout": time_left}

    # Retry logic
    def adjust_settings_after_attempt(_: RetryCallState) -> None:
        settings["receive_timeout"] = max(  # pylint: disable=cell-var-from-loop
            deadline - time.time(), 1
        )

    retry_decorator = retry(
        retry=(
            retry_if_exception_type(ClickhouseError)
            & retry_if_not_exception_message(
                match=rf".*{CLICKHOUSE_TIMEOUT_EXCEEDED_MSG}.*"
            )
        ),
        stop=(stop_after_attempt(max_retries) | stop_after_delay(time_left)),
        wait=wait_random_exponential(max=max_backoff),
        after=adjust_settings_after_attempt,
        reraise=True,
    )

    retry_decorator(execute_query)(
        ctx,
        query,
        format_=None,
        timeout=int(replica_timeout.total_seconds()),
        settings=settings,
    )
