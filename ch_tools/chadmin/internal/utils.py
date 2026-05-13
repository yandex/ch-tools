"""
Utility functions.
"""

import os
import random
import re
import shutil
import threading
import time
from enum import Enum
from itertools import islice
from typing import Any, Iterable, Iterator, Optional

import requests
from click import Context

from ch_tools.common import logging
from ch_tools.common.clickhouse.client.clickhouse_client import clickhouse_client
from ch_tools.common.clickhouse.client.error import ClickhouseError
from ch_tools.common.clickhouse.config.clickhouse import ClickhousePort
from ch_tools.monrun_checks.clickhouse_info import ClickhouseInfo

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class RaisingThread(threading.Thread):

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._exc: Optional[BaseException] = None

    def run(self) -> None:
        try:
            super().run()
        except BaseException as e:
            self._exc = e

    def join(self, timeout: Optional[float] = None) -> None:
        super().join(timeout)
        if self._exc is not None:
            raise self._exc


class Scope(str, Enum):
    """
    Define a queru scope.
    """

    HOST = "host"
    SHARD = "shard"
    CLUSTER = "cluster"


def execute_query(
    ctx: Context,
    query: Any,
    timeout: Optional[int] = None,
    echo: Optional[bool] = False,
    dry_run: Optional[bool] = False,
    format_: Optional[str] = "default",
    stream: bool = False,
    settings: Optional[Any] = None,
    replica: Optional[str] = None,
    log_query: bool = True,
    **kwargs: Any,
) -> Any:
    """
    Execute ClickHouse query.
    """
    if format_ == "default":
        format_ = "PrettyCompact"

    ch_client = clickhouse_client(ctx)

    return ch_client.query(
        query=query,
        query_args=kwargs,
        timeout=timeout,
        format_=format_,
        echo=echo,
        dry_run=dry_run,
        stream=stream,
        settings=settings,
        host=replica,
        log_query=log_query,
    )


def check_replicas_availability(
    ctx: Context,
    timeout: int = 5,
    retry_on_transient_errors: bool = True,
    retry_max_attempts: Optional[int] = None,
    retry_max_interval: Optional[int] = None,
) -> bool:
    """
    Check if all replicas in the shard are available by executing a lightweight query.

    Returns True if all replicas are available, False otherwise.
    Raises an exception if any replica is unavailable with non-retryable error.
    """
    replicas = ClickhouseInfo.get_replicas(ctx)

    # Get retry settings from config if retry is enabled
    max_attempts = (
        retry_max_attempts
        or ctx.obj["config"]["object_storage"]["shard_query_retries"]["max_attempts"]
    )
    max_interval = (
        retry_max_interval
        or ctx.obj["config"]["object_storage"]["shard_query_retries"]["max_interval"]
    )

    for replica in replicas:
        try:
            if retry_on_transient_errors:
                _execute_query_with_retry(
                    ctx,
                    "SELECT 1",
                    replica,
                    timeout=timeout,
                    echo=False,
                    dry_run=False,
                    format_=None,
                    stream=False,
                    settings=None,
                    log_query=False,
                    max_attempts=max_attempts,
                    max_interval=max_interval,
                )
            else:
                execute_query(
                    ctx,
                    "SELECT 1",
                    timeout=timeout,
                    replica=replica,
                    log_query=False,
                )
        except Exception:
            # Replica is unavailable
            logging.error(f"Replica {replica} is unavailable")
            return False

    return True


def _is_retryable_error(error: BaseException) -> bool:
    """
    Determine if an error is transient and can be retried.

    Retryable errors:
    - requests.exceptions.Timeout, ReadTimeout, ConnectTimeout (transient network issues)
    - HTTP 408, 429, 500, 502, 503, 504 from ClickHouse
    - requests.exceptions.ChunkedEncodingError (transient network)

    Non-retryable errors:
    - requests.exceptions.ConnectionError (node unavailable/not started)
    - SQL errors (4xx except 408/429)
    - Schema errors
    """
    # Direct timeout errors are retryable
    if isinstance(
        error, (requests.exceptions.Timeout, requests.exceptions.ReadTimeout)
    ):
        return True

    # ChunkedEncodingError is typically transient
    if isinstance(error, requests.exceptions.ChunkedEncodingError):
        return True

    # ClickHouse HTTP errors - check status code
    if isinstance(error, ClickhouseError):
        retryable_status_codes = {408, 429, 500, 502, 503, 504}
        status_code = error.response.status_code if error.response else None
        if status_code in retryable_status_codes:
            return True

        # Check ClickHouse error code for connection-related errors
        # Code 279: ALL_CONNECTION_TRIES_FAILED - retryable (transient network issue)
        # Code 210: NETWORK_ERROR - retryable (transient network issue)
        error_text = str(error)
        retryable_error_codes = {
            "Code: 279",
            "Code: 210",
            "ALL_CONNECTION_TRIES_FAILED",
            "NETWORK_ERROR",
        }
        if any(code in error_text for code in retryable_error_codes):
            return True

    # ConnectionError means node is unavailable - not retryable
    if isinstance(error, requests.exceptions.ConnectionError):
        return False

    return False


def _execute_query_with_retry(
    ctx: Context,
    query: Any,
    replica: str,
    timeout: Optional[int],
    echo: bool,
    dry_run: bool,
    format_: Optional[str],
    stream: bool,
    settings: Optional[Any],
    log_query: bool,
    max_attempts: int,
    max_interval: int,
    **kwargs: Any,
) -> Any:
    """
    Execute query on a specific replica with retry logic for transient errors.
    """
    last_exception: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        try:
            return execute_query(
                ctx,
                query,
                timeout=timeout,
                echo=echo,
                dry_run=dry_run,
                format_=format_,
                stream=stream,
                settings=settings,
                replica=replica,
                log_query=log_query,
                **kwargs,
            )
        except Exception as e:
            last_exception = e

            if not _is_retryable_error(e):
                # Non-retryable error - fail immediately
                logging.error(
                    f"Query on replica {replica} failed with non-retryable error: {e}"
                )
                raise

            if attempt < max_attempts:
                # Calculate exponential backoff with jitter
                delay = min(
                    max_interval * (0.5 ** (max_attempts - attempt)), max_interval
                )
                jitter = random.uniform(0, delay * 0.1)
                actual_delay = delay + jitter

                logging.warning(
                    f"Query on replica {replica} failed (attempt {attempt}/{max_attempts}): {e}. "
                    f"Retrying in {actual_delay:.2f}s..."
                )
                time.sleep(actual_delay)
            else:
                logging.error(
                    f"Query on replica {replica} failed after {max_attempts} attempts: {e}"
                )

    # All attempts exhausted
    if last_exception:
        raise last_exception
    raise RuntimeError("Unexpected state: no exception but all attempts exhausted")


def execute_query_on_shard(
    ctx: Context,
    query: str,
    timeout: Optional[int] = None,
    echo: Optional[bool] = False,
    dry_run: Optional[bool] = False,
    format_: Optional[str] = "default",
    stream: bool = False,
    settings: Optional[Any] = None,
    log_query: bool = True,
    retry_on_transient_errors: bool = False,
    retry_max_attempts: Optional[int] = None,
    retry_max_interval: Optional[int] = None,
    **kwargs: Any,
) -> None:
    """
    Execute query on all replicas in the shard.

    Args:
        retry_on_transient_errors: Enable retry logic for transient errors (default: False)
        retry_max_attempts: Maximum number of retry attempts (default: from config)
        retry_max_interval: Maximum interval between retries in seconds (default: from config)
    """
    replicas = ClickhouseInfo.get_replicas(ctx)

    # Get retry settings from config if retry is enabled
    max_attempts = (
        retry_max_attempts
        or ctx.obj["config"]["object_storage"]["shard_query_retries"]["max_attempts"]
    )
    max_interval = (
        retry_max_interval
        or ctx.obj["config"]["object_storage"]["shard_query_retries"]["max_interval"]
    )

    if retry_on_transient_errors:

        for replica in replicas:
            _execute_query_with_retry(
                ctx,
                query,
                replica,
                timeout=timeout,
                echo=echo or False,
                dry_run=dry_run or False,
                format_=format_,
                stream=stream,
                settings=settings,
                log_query=log_query,
                max_attempts=max_attempts,
                max_interval=max_interval,
                **kwargs,
            )
    else:
        # Original behavior without retry
        for replica in replicas:
            execute_query(
                ctx,
                query,
                timeout=timeout,
                echo=echo,
                dry_run=dry_run,
                format_=format_,
                stream=stream,
                settings=settings,
                replica=replica,
                log_query=log_query,
                **kwargs,
            )


def get_remote_table_for_hosts(ctx: Context, table: str, replicas: list[str]) -> str:
    """
    Get remote/remoteSecure function for a table with given remote replicas.
    """
    ch_client = clickhouse_client(ctx)
    user_name = ch_client.user or ""

    #  It is believed that all hosts in shard have the same port set, so check current for tcp port
    if ch_client.check_port(ClickhousePort.TCP_SECURE):
        remote_clause = "remoteSecure"
    elif ch_client.check_port(ClickhousePort.TCP):
        remote_clause = "remote"
    else:
        raise RuntimeError("For using remote() table function tcp port must be defined")

    replicas_str = ",".join(replicas)
    return f"{remote_clause}('{replicas_str}', {table}, '{user_name}', '{{user_password}}')"


def format_query(query: str) -> str:
    """
    Format SQL query for output.
    """
    return re.sub(r"(\A|\n)\s*\n", r"\1", query, re.MULTILINE)


def chunked(iterable: Iterable, n: int) -> Iterator[list]:
    """
    Chunkify data into lists of length n. The last chunk may be shorter.

    Based on https://docs.python.org/3/library/itertools.html#itertools-recipes

    >>> chunked('ABCDEFG', 3)
    ABC DEF G
    """
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)

    while True:
        chunk = list(islice(it, n))
        if not chunk:
            break
        yield chunk


def replace_macros(string: str, macros: dict) -> str:
    """
    Substitute macros in the specified string. Macros in string are specified in the form "{macro_name}".

    Example:
    >>> replace_macros('{a} and {b}', {'a': '1', 'b': '2'})
    1 and 2
    """
    return re.sub(
        string=string,
        pattern=r"{([^{}]+)}",
        repl=lambda m: macros.get(m.group(1), m.group(0)),
    )


def remove_from_disk(path: str) -> None:
    """
    Remove file or directory with its all content.

    Behaviour is similar to 'rm -rf'.

    Args:
        path: Path to file or directory to remove
    Raises:
        OSError: If removal fails due to permissions or other system errors
    """
    logging.info(f"Removing path: {path}")

    if os.path.isfile(path) or os.path.islink(path):
        os.remove(path)
    elif os.path.isdir(path):
        shutil.rmtree(path)
    else:
        # Path doesn't exist, which is equivalent to successful rm -rf
        pass


def get_table_function_for_scope(
    ctx: Context,
    table: str,
    scope: Scope,
    cluster_name: Optional[str] = None,
    all_replicas: bool = False,
) -> str:
    """
    Get correct table function for given scope.
    """
    if scope == Scope.CLUSTER:
        assert cluster_name
        if all_replicas:
            return f"clusterAllReplicas('{cluster_name}', {table})"
        return f"cluster('{cluster_name}', {table})"
    if scope == Scope.SHARD:
        return get_remote_table_for_hosts(ctx, table, ClickhouseInfo.get_replicas(ctx))

    return table


def assert_equal_table_schema_on_cluster(
    ctx: Context, database: str, table: str, cluster: str
) -> None:
    """
    Retrieve table schema from cluster and compare it with local table schema.
    """
    get_local_schema_query = """
                            SELECT
                                hostName() AS host,
                                create_table_query
                            FROM {tables_table}
                            WHERE database = '{database}'
                            AND name = '{table}'"""

    # Join is needed in case table doesn't exist on some replicas
    get_cluster_schema_query = """
                        SELECT
                            replicas.host,
                            coalesce(t.create_table_query, '') AS create_table_query
                        FROM
                        (
                            SELECT hostName() AS host
                            FROM clusterAllReplicas('{cluster}', system.one)
                        ) AS replicas
                        LEFT JOIN
                        (
                            {get_local_schema_query}
                        ) AS t USING host
                        """

    get_local_schema = get_local_schema_query.format(
        tables_table="system.tables",
        database=database,
        table=table,
    )
    get_cluster_schema = get_cluster_schema_query.format(
        get_local_schema_query=get_local_schema_query.format(
            tables_table=get_table_function_for_scope(
                ctx,
                "system.tables",
                Scope.CLUSTER,
                cluster_name=cluster,
                all_replicas=True,
            ),
            database=database,
            table=table,
        ),
        cluster=cluster,
    )

    local_schema = execute_query(ctx, get_local_schema, format_="JSONCompact")["data"][
        0
    ][1]
    schemas = execute_query(ctx, get_cluster_schema, format_="JSONCompact")["data"]

    for row in schemas:
        replica = row[0]
        schema = row[1]

        logging.debug(f"Replica: {replica}, schema: {schema}")

        if local_schema != schema:
            full_table_name = f"{database}.{table}"
            raise RuntimeError(
                f"Table schema for '{full_table_name}' on replica '{replica}' is different from local schema. Local schema: {local_schema}, replica schema: {schema}"
            )
