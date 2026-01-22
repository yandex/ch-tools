import os
import re
import subprocess
import tarfile
import threading
import uuid
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, Generator, Iterator, List, Optional, Tuple

from click import Context
from humanfriendly import format_size

from ch_tools.chadmin.internal.object_storage import cleanup_s3_object_storage
from ch_tools.chadmin.internal.object_storage.obj_list_item import ObjListItem
from ch_tools.chadmin.internal.object_storage.s3_cleanup_stats import (
    ResultStat,
    StatisticsPeriod,
)
from ch_tools.chadmin.internal.object_storage.s3_iterator import (
    s3_object_storage_iterator,
)
from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    S3ObjectLocalInfo,
    S3ObjectLocalMetaData,
)
from ch_tools.chadmin.internal.system import match_ch_backup_version, match_ch_version
from ch_tools.chadmin.internal.table import (
    delete_table_by_full_name,
    get_table,
    list_tables,
    table_exists,
)
from ch_tools.chadmin.internal.utils import (
    DATETIME_FORMAT,
    chunked,
    execute_query,
    execute_query_on_shard,
    get_remote_table_for_hosts,
)
from ch_tools.chadmin.internal.zookeeper import has_zk
from ch_tools.common import logging
from ch_tools.common.backup import get_missing_chs3_backups
from ch_tools.common.clickhouse.client.clickhouse_client import (
    ClickhouseClient,
    clickhouse_client,
)
from ch_tools.common.clickhouse.client.query import Query
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration
from ch_tools.monrun_checks.clickhouse_info import ClickhouseInfo

# Batch size for inserts in a listing table
# Set not very big value due to default ClickHouse 'http_max_field_value_size' settings value 128Kb
# TODO: streaming upload in POST body while INSERT
INSERT_BATCH_SIZE = 500
# The guard interval is used for S3 objects for which metadata is not found.
# And for metadata for which object is not found in S3.
# These objects are not counted if their last modified time fall in the interval from the moment of starting analyzing.
DEFAULT_GUARD_INTERVAL = "24h"
# Batch size for reading from orphaned objects table
# corresponds to chunk size in s3_cleanup
KEYS_BATCH_SIZE = 1000
REMOTE_DATA_PATHS_TABLE = "system.remote_data_paths"
ZOOKEEPER_ARGS = "('{replica_zk_prefix}/{{shard}}/{table_name}', '{{replica}}')"

# Patterns for identifying unique service tables (with UUID and timestamp)
UNIQUE_TABLE_UUID_PATTERN = (
    r"[a-f0-9]{8}_[a-f0-9]{4}_[a-f0-9]{4}_[a-f0-9]{4}_[a-f0-9]{12}"
)
UNIQUE_TABLE_TIMESTAMP_PATTERN = r"\d{8}_\d{6}"
UNIQUE_TABLE_SUFFIX_PATTERN = (
    f"_{UNIQUE_TABLE_UUID_PATTERN}_({UNIQUE_TABLE_TIMESTAMP_PATTERN})"
)
# Precompiled regex patterns for better performance
UNIQUE_TABLE_SUFFIX_REGEX = re.compile(UNIQUE_TABLE_SUFFIX_PATTERN)
# Timestamp format for parsing table timestamps
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
# Can be used to check if schema is changed and table needs to be recreated forcefully
REMOTE_BLOBS_COLUMNS = "last_modified DateTime, obj_path String, obj_size UInt64"
UNKNOWN_REPLICAS_NAME = "unknown_replicas"


class Scope(str, Enum):
    """
    Define a local metadata search scope.
    """

    HOST = "host"
    SHARD = "shard"
    CLUSTER = "cluster"


class State(str, Enum):
    """
    Possible states for blobls.
    """

    ACTIVE = "active"
    UNIQUE_FROZEN = "unique_frozen"
    UNIQUE_DETACHED = "unique_detached"
    ORPHANED = "orphaned"


class LocalState(str, Enum):
    """
    Possible directories for s3 parts.
    """

    ACTIVE = "active"
    SHADOW = "shadow"
    DETACHED = "detached"


def clean(
    ctx: Context,
    object_name_prefix: str,
    from_time: Optional[timedelta],
    to_time: timedelta,
    dry_run: bool,
    keep_paths: bool,
    use_saved_list: bool,
    verify_paths_regex: Optional[str] = None,
    max_size_to_delete_bytes: int = 0,
    max_size_to_delete_fraction: float = 1.0,
    ignore_missing_cloud_storage_backups: bool = False,
    stat_partitioning: StatisticsPeriod = StatisticsPeriod.ALL,
) -> ResultStat:
    """
    Clean orphaned S3 objects.
    """
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]
    config = ctx.obj["config"]["object_storage"]["space_usage"]
    ch_client = clickhouse_client(ctx)

    _cleanup_old_service_tables(ctx)

    result_stat = ResultStat(stat_partitioning)

    if dry_run:
        logging.info("Counting orphaned objects...")
    else:
        logging.info("Deleting orphaned objects...")

    with _get_blobs_tables(
        ctx,
        object_name_prefix,
        from_time,
        to_time,
        unique_name=not keep_paths,
        keep_table=keep_paths,
        use_saved=use_saved_list,
        ignore_missing_cloud_storage_backups=ignore_missing_cloud_storage_backups,
    ) as (remote_blobs_table, local_blobs_table, orphaned_blobs_table):
        timeout = config["antijoin_timeout"]
        query_settings = {"receive_timeout": timeout, "max_execution_time": 0}
        orphaned_objects_iterator = _object_list_generator(
            ctx, orphaned_blobs_table, from_time, to_time, query_settings, timeout
        )
        listing_size_in_bucket = int(
            ch_client.query_json_data_first_row(
                query=f"SELECT sum(obj_size) FROM {remote_blobs_table}",
                compact=True,
            )[0]
        )

        # Safety checks
        if ctx.obj["config"]["object_storage"]["clean"]["verify"]:
            _sanity_check_before_cleanup(
                ctx,
                orphaned_objects_iterator,
                ch_client,
                listing_size_in_bucket,
                orphaned_blobs_table,
                verify_paths_regex,
                dry_run,
            )

        result_stat = _cleanup_orphaned_objects(
            orphaned_objects_iterator,
            disk_conf,
            listing_size_in_bucket,
            max_size_to_delete_bytes,
            max_size_to_delete_fraction,
            stat_partitioning,
            dry_run,
        )

    return result_stat


def collect_object_storage_info(
    ctx: Context,
    object_name_prefix: str,
    from_time: Optional[timedelta],
    to_time: timedelta,
    use_saved: bool = False,
) -> None:
    """
    Collects object storage usage information.
    Uses temporary unique tables for all blob tables with automatic deletion.
    """
    _cleanup_old_service_tables(ctx)

    with _get_blobs_tables(
        ctx,
        object_name_prefix,
        from_time,
        to_time,
        unique_name=False,
        keep_table=True,
        use_saved=use_saved,
    ) as (remote_blobs_table, local_blobs_table, orphaned_blobs_table):
        _collect_space_usage(ctx, local_blobs_table, orphaned_blobs_table)


def get_object_storage_space_usage(
    ctx: Context,
    cluster_name: str,
    scope: Scope,
) -> dict:
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]
    config = ctx.obj["config"]["object_storage"]["space_usage"]
    space_usage_table = (
        f"{config['database']}.{config['space_usage_table_prefix']}{disk_conf.name}"
    )

    # Table is replicated
    if scope == Scope.SHARD:
        scope = Scope.HOST
    space_usage_table = _get_table_function(ctx, space_usage_table, scope, cluster_name)

    parts_usage_query = Query(
        f"SELECT toString(replica) replicas, state, size FROM {space_usage_table} ORDER BY replica, state"
    )

    query_result = execute_query(
        ctx,
        parts_usage_query,
        format_="JSON",
    )["data"]

    result: dict[str, dict[str, int]] = defaultdict(dict)
    for row in query_result:
        result[row["replicas"]][row["state"]] = row["size"]

    result["total"] = defaultdict(int)
    for replica in result:
        if replica == "total":
            continue
        for state in result[replica]:
            result["total"][state] += result[replica][state]

    # Single replicas first, special keys last
    # (special, replica count, replica name)
    def sort_key(item: tuple[str, dict]) -> tuple[int, int, str]:
        key = item[0]
        if key in ("total", f"['{UNKNOWN_REPLICAS_NAME}']"):
            return (1, 0, key)
        replica_count = key.count("','") + 1 if key.startswith("[") else 0
        return (0, replica_count, key)

    sorted_result = dict(sorted(result.items(), key=sort_key))
    return sorted_result


def _calculate_size_limits(
    listing_size_in_bucket: int,
    max_size_to_delete_bytes: int,
    max_size_to_delete_fraction: float,
) -> int:
    """
    Calculates the maximum size of objects to delete.
    """
    max_size_to_delete = int(listing_size_in_bucket * max_size_to_delete_fraction)
    if max_size_to_delete_bytes:
        max_size_to_delete = min(max_size_to_delete, max_size_to_delete_bytes)
    return max_size_to_delete


def _process_objects_batch(
    objects: List[ObjListItem],
    max_size_to_delete: int,
    result_stat: ResultStat,
) -> Tuple[List[ObjListItem], bool]:
    """
    Processes a batch of objects, fitting them to size constraints.
    Returns (processed_objects, is_last_batch).
    """
    batch_size = sum(obj.size for obj in objects)
    is_last_batch = False

    if result_stat.total["total_size"] + batch_size > max_size_to_delete:
        # To fit into the size restriction: sort all objects by size
        # And remove elements from the end
        is_last_batch = True
        objects = sorted(objects, key=lambda obj: obj.size)
        while result_stat.total["total_size"] + batch_size > max_size_to_delete:
            batch_size -= objects[-1].size
            objects.pop()

    return objects, is_last_batch


def _cleanup_orphaned_objects(
    orphaned_objects_iterator: Callable,
    disk_conf: S3DiskConfiguration,
    listing_size_in_bucket: int,
    max_size_to_delete_bytes: int,
    max_size_to_delete_fraction: float,
    stat_partitioning: StatisticsPeriod,
    dry_run: bool,
) -> ResultStat:
    """
    Performs the main logic for cleaning up orphaned objects.
    """
    result_stat = ResultStat(stat_partitioning)

    max_size_to_delete = _calculate_size_limits(
        listing_size_in_bucket, max_size_to_delete_bytes, max_size_to_delete_fraction
    )

    for objects in chunked(orphaned_objects_iterator(), KEYS_BATCH_SIZE):
        objects, is_last_batch = _process_objects_batch(
            objects, max_size_to_delete, result_stat
        )

        cleanup_s3_object_storage(disk_conf, iter(objects), result_stat, dry_run)

        if is_last_batch:
            break

    deleted = result_stat.total["deleted"]
    total_size = format_size(result_stat.total["total_size"], binary=True)
    logging.info(
        f"{'Would delete' if dry_run else 'Deleted'} {deleted} objects with total size {total_size} from bucket [{disk_conf.bucket_name}]",
    )

    return result_stat


def _cleanup_old_service_tables(ctx: Context) -> None:
    """
    Remove old service tables (local_blobs, orphaned_blobs, remote_blobs) that have unique names
    (contain UUID and timestamp) and are older than configured retention period.
    This helps prevent accumulation of temporary tables from previous runs.
    """
    config = ctx.obj["config"]["object_storage"]["space_usage"]
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]

    retention_days = config["service_tables_retention_days"]
    retention_threshold = datetime.now(timezone.utc) - timedelta(days=retention_days)

    table_prefixes = [
        config["local_blobs_table_prefix"],
        config["orphaned_blobs_table_prefix"],
        config["remote_blobs_table_prefix"],
    ]

    database = config["database"]

    for table_prefix in table_prefixes:
        try:
            base_name = f"{table_prefix}{disk_conf.name}"
            table_pattern = f"{base_name}_%"

            tables = list_tables(
                ctx, database_name=database, table_pattern=table_pattern
            )

            for table_info in tables:
                table_name = table_info["name"]

                if match := UNIQUE_TABLE_SUFFIX_REGEX.search(table_name):
                    table_timestamp_str = match.group(1)
                    table_timestamp = datetime.strptime(
                        table_timestamp_str, TIMESTAMP_FORMAT
                    )
                    table_timestamp = table_timestamp.replace(tzinfo=timezone.utc)

                    # Delete table if it's older than retention period
                    if table_timestamp < retention_threshold:
                        full_table_name = f"{database}.{table_name}"
                        logging.info(f"Removing old service table: {full_table_name}")

                        if table_prefix == config["remote_blobs_table_prefix"]:
                            delete_table_by_full_name(ctx, full_table_name, shard=True)
                        else:
                            delete_table_by_full_name(ctx, full_table_name)
        except:
            logging.exception(f"Error cleaning up old {table_prefix} tables:")
            raise


def _get_table_name(ctx: Context, table_prefix: str, unique_name: bool = False) -> str:
    """
    Generates table name with UUID+timestamp if unique_name=True.
    """
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]
    config = ctx.obj["config"]["object_storage"]["space_usage"]

    base_name = f"{config['database']}.{table_prefix}{disk_conf.name}"

    if not unique_name:
        return base_name

    table_uuid = str(uuid.uuid4()).replace("-", "_")
    timestamp = datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT)
    return f"{base_name}_{table_uuid}_{timestamp}"


@contextmanager
def _get_blobs_tables(
    ctx: Context,
    object_name_prefix: str,
    from_time: Optional[timedelta],
    to_time: timedelta,
    unique_name: bool = False,
    keep_table: bool = True,
    use_saved: bool = False,
    ignore_missing_cloud_storage_backups: bool = False,
) -> Generator[Tuple[str, str, str], None, None]:
    """
    Returns tuple: (remote_blobs_table, local_blobs_table, orphaned_blobs_table)
    Manages cloud storage backups automatically.
    """
    with (
        _remote_blobs_table(
            ctx,
            object_name_prefix,
            from_time,
            to_time,
            unique_name=unique_name,
            keep_table=keep_table,
            use_saved=use_saved,
        ) as remote_blobs_table,
        _local_blobs_table(
            ctx,
            ignore_missing_cloud_storage_backups=ignore_missing_cloud_storage_backups,
        ) as local_blobs_table,
        _orphaned_blobs_table(
            ctx, local_blobs_table, remote_blobs_table
        ) as orphaned_blobs_table,
    ):
        yield remote_blobs_table, local_blobs_table, orphaned_blobs_table


@contextmanager
def _local_blobs_table(
    ctx: Context,
    unique_name: bool = True,
    keep_table: bool = True,
    use_saved: bool = False,
    ignore_missing_cloud_storage_backups: bool = False,
) -> Generator[str, None, None]:
    """
    Context manager for creating and managing the local_blobs table.
    """
    config = ctx.obj["config"]["object_storage"]["space_usage"]
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]

    local_blobs_table = _get_table_name(
        ctx, config["local_blobs_table_prefix"], unique_name
    )

    try:
        if use_saved:
            table_name_only = local_blobs_table.split(".")[1]
            if not table_exists(ctx, config["database"], table_name_only):
                raise RuntimeError(
                    f"Can't use saved {local_blobs_table} because it does not exist"
                )
        else:
            _create_local_blobs_table(
                ctx,
                local_blobs_table,
                config["storage_policy"],
                drop_existing_table=True,
            )

            remote_data_paths_table = _get_table_function(
                ctx, REMOTE_DATA_PATHS_TABLE, Scope.SHARD, cluster_name=None
            )

            local_blobs_query = _get_fill_local_blobs_table_query(
                ctx, local_blobs_table, remote_data_paths_table, disk_conf.name
            )

            timeout = config["antijoin_timeout"]
            query_settings = {"receive_timeout": timeout, "max_execution_time": 0}
            execute_query(
                ctx,
                local_blobs_query,
                timeout=timeout,
                settings=query_settings,
            )

            if not ignore_missing_cloud_storage_backups:
                _insert_missing_s3_backups_blobs(ctx, local_blobs_table, disk_conf)

        yield local_blobs_table
    finally:
        if not keep_table:
            delete_table_by_full_name(ctx, local_blobs_table)


def _insert_missing_s3_backups_blobs(
    ctx: Context,
    local_blobs_table: str,
    disk_conf: S3DiskConfiguration,
) -> None:
    """
    Download cloud storage metadata for missing backups and put it to local_blobs_table.
    """

    def _insert_blobs_from_tar(pipe_path: str) -> None:
        def _generate_blobs_from_tar_files() -> Iterator[S3ObjectLocalInfo]:
            with open(pipe_path, "rb") as pipe:
                with tarfile.open(fileobj=pipe, mode="r|*") as tar:
                    for member in tar:
                        file = tar.extractfile(member)
                        if file:
                            data = file.read().decode("utf-8")
                            yield from S3ObjectLocalMetaData.from_string(data).objects

        for metadata_list in chunked(
            _generate_blobs_from_tar_files(), INSERT_BATCH_SIZE
        ):
            _insert_local_blobs_batch(
                ctx,
                metadata_list,
                local_blobs_table,
                UNKNOWN_REPLICAS_NAME,
                LocalState.SHADOW,
                disk_conf,
            )

    missing_backups = get_missing_chs3_backups(disk_conf.name)

    for backup in missing_backups:
        if not match_ch_backup_version("2.651.159295191"):
            logging.warning(
                "Skip downloading missing backups. Reason: Download metadata command is available since ch-backup 2.651.159295191"
            )
            return

        logging.info(
            f"Downloading cloud storage metadata from missing backup '{backup}'"
        )

        config = ctx.obj["config"]["object_storage"]["space_usage"][
            "download_missing_cloud_storage_backups"
        ]
        pipe_path = config["named_pipe_path"]
        timeout = config["timeout"]
        if os.path.exists(pipe_path):
            raise RuntimeError(f"Pipe at {pipe_path} already exists")

        with _missing_backups_named_pipe(pipe_path):
            parse_thread = threading.Thread(
                target=_insert_blobs_from_tar, args=(pipe_path,), daemon=True
            )
            parse_thread.start()

            cmd = [
                "ch-backup",
                "get-cloud-storage-metadata",
                "--disk",
                disk_conf.name,
                "--local-path",
                pipe_path,
                backup,
            ]
            proc = subprocess.Popen(
                cmd,
                shell=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            _, stderr = proc.communicate(timeout)
            if proc.returncode:
                assert proc.stderr
                raise RuntimeError(
                    f"Downloading cloud storage metadata command has failed: retcode {proc.returncode}, stderr: {stderr.decode('utf-8')}"
                )

            parse_thread.join(timeout)
            if parse_thread.is_alive():
                raise RuntimeError(
                    "Downloading cloud storage metadata command has failed: Timeout exceeded, metadata reading thread is probably locked"
                )


@contextmanager
def _missing_backups_named_pipe(
    pipe_path: str,
) -> Generator[None, None, None]:
    """
    Context manager for creating and managing the named pipe to read missing backups from ch-backup.
    """
    os.mkfifo(pipe_path)
    # mkfifo with mode parameter doesn't work correctly
    os.chmod(pipe_path, 0o666)
    try:
        yield
    finally:
        os.remove(pipe_path)


@contextmanager
def _orphaned_blobs_table(
    ctx: Context,
    local_blobs_table: str,
    remote_blobs_table: str,
    unique_name: bool = True,
    keep_table: bool = True,
    use_saved: bool = False,
) -> Generator[str, None, None]:
    """
    Context manager for creating and managing the orphaned_blobs table.
    """
    config = ctx.obj["config"]["object_storage"]["space_usage"]

    orphaned_blobs_table = _get_table_name(
        ctx, config["orphaned_blobs_table_prefix"], unique_name
    )

    try:
        if use_saved:
            table_name_only = orphaned_blobs_table.split(".")[1]
            if not table_exists(ctx, config["database"], table_name_only):
                raise RuntimeError(
                    f"Can't use saved {orphaned_blobs_table} because it does not exist"
                )
        else:
            _create_orphaned_blobs_table(
                ctx,
                orphaned_blobs_table,
                config["storage_policy"],
                drop_existing_table=True,
            )

            _collect_orphaned_blobs(
                ctx, local_blobs_table, orphaned_blobs_table, remote_blobs_table
            )

        yield orphaned_blobs_table
    finally:
        if not keep_table:
            delete_table_by_full_name(ctx, orphaned_blobs_table)


@contextmanager
def _remote_blobs_table(
    ctx: Context,
    object_name_prefix: str,
    from_time: Optional[timedelta],
    to_time: timedelta,
    unique_name: bool = False,
    keep_table: bool = True,
    use_saved: bool = False,
) -> Generator[str, None, None]:
    """
    Context manager for creating and managing the remote_blobs table.
    """
    config = ctx.obj["config"]["object_storage"]["space_usage"]

    remote_blobs_table = _get_table_name(
        ctx, config["remote_blobs_table_prefix"], unique_name
    )

    recreate_table = not use_saved

    try:
        if use_saved:
            table_name_only = remote_blobs_table.split(".")[1]
            if not table_exists(ctx, config["database"], table_name_only):
                raise RuntimeError(
                    f"Can't use saved {remote_blobs_table} because it does not exist"
                )

            table_create_statement = get_table(
                ctx, config["database"], table_name_only
            )["create_table_query"]
            if not _check_schema_mismatch_for_remote_blobs(table_create_statement):
                logging.warning(
                    f"Existing table {remote_blobs_table} has outdated schema, will recreate it"
                )
                recreate_table = True

        if recreate_table:
            _create_remote_blobs_table(
                ctx,
                remote_blobs_table,
                config["zk_path_prefix"],
                config["storage_policy"],
                replicated=has_zk()
                and not unique_name,  # Table with unique name is supposed to be temporary and should not be replicated
                drop_existing_table=True,
            )
            _collect_remote_blobs(
                ctx, from_time, to_time, object_name_prefix, remote_blobs_table
            )

        yield remote_blobs_table
    finally:
        if not keep_table:
            delete_table_by_full_name(ctx, remote_blobs_table, shard=True)


def _collect_orphaned_blobs(
    ctx: Context,
    local_blobs_table: str,
    orphaned_blobs_table: str,
    remote_blobs_table: str,
) -> None:
    config = ctx.obj["config"]["object_storage"]["space_usage"]

    ch_client = clickhouse_client(ctx)
    user_password = ch_client.password or ""

    antijoin_query = Query(
        f"""
        INSERT INTO {orphaned_blobs_table}
            SELECT last_modified, obj_path, obj_size FROM {remote_blobs_table} AS object_storage
            LEFT ANTI JOIN {local_blobs_table} AS object_table
            ON object_table.obj_path = object_storage.obj_path
        """,
        sensitive_args={"user_password": user_password},
    )
    logging.info("Antijoin query: {}", str(antijoin_query))

    timeout = config["antijoin_timeout"]
    query_settings = {"receive_timeout": timeout, "max_execution_time": 0}
    execute_query(
        ctx,
        antijoin_query,
        timeout=timeout,
        settings=query_settings,
    )


def _collect_space_usage(
    ctx: Context,
    local_blobs_table: str,
    orphaned_blobs_table: str,
) -> None:
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]
    config = ctx.obj["config"]["object_storage"]["space_usage"]
    space_usage_table = (
        f"{config['database']}.{config['space_usage_table_prefix']}{disk_conf.name}"
    )
    space_usage_table_new = f"{space_usage_table}_new"

    # This table will store the actual space usage data after the query
    _create_space_usage_table(
        ctx,
        space_usage_table_name=space_usage_table_new,
        replica_zk_prefix=config["zk_path_prefix"],
        storage_policy=config["storage_policy"],
        drop_existing_table=True,
    )
    # This table should always exist and store data from the previous run
    _create_space_usage_table(
        ctx,
        space_usage_table_name=space_usage_table,
        replica_zk_prefix=config["zk_path_prefix"],
        storage_policy=config["storage_policy"],
        drop_existing_table=False,
    )

    space_usage_queries = _get_fill_space_usage_query(
        ctx, local_blobs_table, orphaned_blobs_table, space_usage_table_new
    )

    # TODO: should probably use different setting for timeout
    timeout = config["antijoin_timeout"]
    query_settings = {"receive_timeout": timeout, "max_execution_time": 0}
    for query in space_usage_queries:
        execute_query(
            ctx,
            query,
            timeout=timeout,
            settings=query_settings,
        )

    execute_query(ctx, f"TRUNCATE TABLE {space_usage_table} SYNC")
    execute_query(
        ctx, f"INSERT INTO {space_usage_table} SELECT * FROM {space_usage_table_new}"
    )
    delete_table_by_full_name(ctx, space_usage_table_new, shard=True)


def _object_list_generator(
    ctx: Context,
    table_name: str,
    from_time: Optional[timedelta],
    to_time: Optional[timedelta],
    query_settings: Dict[str, Any],
    timeout: Optional[int] = None,
) -> Callable:
    now = datetime.now(timezone.utc)
    from_time_cond = (
        (now - from_time).strftime(DATETIME_FORMAT) if from_time is not None else None
    )
    to_time_cond = (
        (now - to_time).strftime(DATETIME_FORMAT) if to_time is not None else None
    )

    query = """
        SELECT * FROM {{ table_name }}
        WHERE 1
        {%- if from_time_cond %}
            AND last_modified >= toDateTime('{{ from_time_cond }}')
        {%- endif %}
        {%- if to_time_cond %}
            AND last_modified <= toDateTime('{{ to_time_cond }}')
        {%- endif %}
        ORDER BY last_modified
        """

    def obj_list_iterator() -> Iterator[ObjListItem]:
        with execute_query(
            ctx,
            query,
            format_="JSONEachRow",
            timeout=timeout,
            settings=query_settings,
            stream=True,
            table_name=table_name,
            from_time_cond=from_time_cond,
            to_time_cond=to_time_cond,
        ) as resp:
            for line in resp.iter_lines():
                yield ObjListItem.from_json(line)

    return obj_list_iterator


def _sanity_check_before_cleanup(
    ctx: Context,
    orphaned_objects_iterator: Callable,
    ch_client: ClickhouseClient,
    listing_size_in_bucket: int,
    orphaned_blobs_table: str,
    verify_paths_regex: Optional[str],
    dry_run: bool,
) -> None:
    """
    Performs safety checks before deleting objects.
    Uses orphaned_blobs_table to check paths and size of objects to delete.
    """
    size_error_rate_threshold_fraction = ctx.obj["config"]["object_storage"]["clean"][
        "verify_size_error_rate_threshold_fraction"
    ]
    if verify_paths_regex:
        paths_regex = verify_paths_regex
    else:
        paths_regex = ctx.obj["config"]["object_storage"]["clean"][
            "verify_paths_regex"
        ]["shard"]

    paths_regex_compiled = re.compile(paths_regex)

    def raise_or_warn(dry_run: bool, msg: str) -> None:
        if not dry_run:
            raise RuntimeError(msg)

        logging.warning(f"Warning: {msg}")

    def perform_check_paths() -> None:
        """
        Validate that object paths match expected regex pattern.
        This prevents accidental deletion of objects with unexpected paths.
        """
        orphaned_objects_count = int(
            ch_client.query_json_data_first_row(
                query=Query(
                    f"SELECT count() FROM {orphaned_blobs_table}",
                    sensitive_args={"user_password": ch_client.password or ""},
                ),
                compact=True,
            )[0]
        )

        # Skip validation if no objects to check
        if orphaned_objects_count == 0:
            return

        # Get a sample path from database for initial validation
        sample_path = ch_client.query_json_data_first_row(
            query=Query(
                f"SELECT obj_path FROM {orphaned_blobs_table} LIMIT 1",
                sensitive_args={"user_password": ch_client.password or ""},
            ),
            compact=True,
        )[0]

        if not paths_regex_compiled.match(sample_path):
            raise_or_warn(
                dry_run,
                f"Path validation failed: sample path '{sample_path}' doesn't match regex '{paths_regex}'",
            )

        for orphaned_object in orphaned_objects_iterator():
            if not paths_regex_compiled.match(orphaned_object.path):
                raise_or_warn(
                    dry_run,
                    f"Path validation failed: object path '{orphaned_object.path}' doesn't match regex '{paths_regex}'",
                )

    def perform_check_size() -> None:
        """
        Validate that deletion size doesn't exceed safety threshold.
        This prevents accidental deletion of too much data.
        """
        if listing_size_in_bucket == 0:
            return

        # Calculate total size of objects to be deleted
        total_size_to_delete = 0
        for orphaned_object in orphaned_objects_iterator():
            total_size_to_delete += orphaned_object.size

        # Check if deletion size exceeds safety threshold
        if (
            total_size_to_delete
            >= listing_size_in_bucket * size_error_rate_threshold_fraction
        ):
            threshold_percentage = int(size_error_rate_threshold_fraction * 100)
            raise_or_warn(
                dry_run,
                f"Size validation failed: attempting to delete {format_size(total_size_to_delete)} "
                f"which is more than {threshold_percentage}% of total bucket size {format_size(listing_size_in_bucket)}",
            )

    perform_check_paths()

    # Size validation is only available in ClickHouse 23.3+ (when 'size' column exists in system.remote_data_paths)
    if match_ch_version(ctx, min_version="23.3"):
        perform_check_size()


def _collect_remote_blobs(
    ctx: Context,
    from_time: Optional[timedelta],
    to_time: timedelta,
    object_name_prefix: str,
    remote_blobs_table: str,
) -> None:
    """
    Traverse S3 disk's bucket and put object names to the ClickHouse table.
    """
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]

    prefix = object_name_prefix or disk_conf.prefix
    prefix = os.path.join(prefix, "")

    logging.info(
        f"Collecting objects... (Disk: '{disk_conf.name}', Endpoint '{disk_conf.endpoint_url}', Bucket: '{disk_conf.bucket_name}', Prefix: '{prefix}')",
    )

    obj_paths_batch: List[ObjListItem] = []
    counter = 0
    now = datetime.now(timezone.utc)
    for obj in s3_object_storage_iterator(
        ctx.obj["disk_configuration"], object_name_prefix=prefix
    ):
        if obj.last_modified > now - to_time:
            continue
        if from_time is not None and obj.last_modified < now - from_time:
            continue

        obj_paths_batch.append(ObjListItem(obj.last_modified, obj.key, obj.size))
        counter += 1
        if len(obj_paths_batch) >= INSERT_BATCH_SIZE:
            _insert_remote_blobs_batch(ctx, obj_paths_batch, remote_blobs_table)
            obj_paths_batch.clear()

    # Insert the last batch (might be shorter)
    if obj_paths_batch:
        _insert_remote_blobs_batch(ctx, obj_paths_batch, remote_blobs_table)

    logging.info("Collected {} objects", counter)


def _insert_remote_blobs_batch(
    ctx: Context, obj_paths_batch: List[ObjListItem], remote_blobs_table: str
) -> None:
    """
    Insert batch of object names to the listing table.
    """
    batch_values = ",".join(
        f"('{item.last_modified.strftime(DATETIME_FORMAT)}','{item.path}',{item.size})"
        for item in obj_paths_batch
    )
    execute_query(
        ctx,
        f"INSERT INTO {remote_blobs_table} (last_modified, obj_path, obj_size) VALUES {batch_values}",
        format_=None,
    )


def _insert_local_blobs_batch(
    ctx: Context,
    obj_paths_batch: List[S3ObjectLocalInfo],
    local_blobs_table: str,
    replica: str,
    state: LocalState,
    disk_conf: S3DiskConfiguration,
) -> None:
    """
    Insert batch of object names to the listing table.
    """
    batch_values = ",".join(
        f"('{replica}','{item.key if item.key_is_full else os.path.join(disk_conf.prefix, item.key)}','{state}',{item.size},1)"
        for item in obj_paths_batch
    )
    execute_query(
        ctx,
        f"INSERT INTO {local_blobs_table} (replica, obj_path, state, obj_size, ref_count) VALUES {batch_values}",
        format_=None,
    )


def _create_remote_blobs_table(
    ctx: Context,
    table_name: str,
    replica_zk_prefix: str,
    storage_policy: str,
    replicated: bool,
    drop_existing_table: bool = False,
) -> None:
    if drop_existing_table:
        delete_table_by_full_name(ctx, table_name, shard=True)

    engine = (
        "ReplicatedMergeTree"
        + ZOOKEEPER_ARGS.format(
            replica_zk_prefix=replica_zk_prefix, table_name=table_name
        )
        if replicated
        else "MergeTree"
    )

    execute_query_on_shard(
        ctx,
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
            ({REMOTE_BLOBS_COLUMNS})
            ENGINE {engine}
            ORDER BY (last_modified, obj_path)
            SETTINGS storage_policy = '{storage_policy}'
        """,
        format_=None,
    )


def _create_local_blobs_table(
    ctx: Context,
    table_name: str,
    storage_policy: str,
    drop_existing_table: bool,
) -> None:
    if drop_existing_table:
        delete_table_by_full_name(ctx, table_name)

    execute_query(
        ctx,
        f"""
            CREATE TABLE IF NOT EXISTS {table_name}
                (replica String,
                obj_path String,
                state Enum('{LocalState.ACTIVE}', '{LocalState.SHADOW}', '{LocalState.DETACHED}'),
                obj_size UInt64,
                ref_count UInt32)
                ENGINE MergeTree
                ORDER BY (replica, obj_path, state)
                SETTINGS storage_policy = '{storage_policy}'
        """,
        format_=None,
    )


def _create_orphaned_blobs_table(
    ctx: Context,
    table_name: str,
    storage_policy: str,
    drop_existing_table: bool,
) -> None:
    if drop_existing_table:
        delete_table_by_full_name(ctx, table_name)

    execute_query(
        ctx,
        f"""
            CREATE TABLE IF NOT EXISTS {table_name}
                (last_modified DateTime, obj_path String, obj_size UInt64)
                ENGINE MergeTree
                ORDER BY obj_path
                SETTINGS storage_policy = '{storage_policy}'
        """,
        format_=None,
    )


def _create_space_usage_table(
    ctx: Context,
    space_usage_table_name: str,
    replica_zk_prefix: str,
    storage_policy: str,
    drop_existing_table: bool = True,
) -> None:
    if drop_existing_table:
        delete_table_by_full_name(ctx, space_usage_table_name, shard=True)

    engine = (
        "ReplicatedMergeTree"
        + ZOOKEEPER_ARGS.format(
            replica_zk_prefix=replica_zk_prefix, table_name=space_usage_table_name
        )
        if has_zk()
        else "MergeTree"
    )

    execute_query_on_shard(
        ctx,
        f"""
            CREATE TABLE IF NOT EXISTS {space_usage_table_name}
                (replica Array(String),
                state Enum('{State.ACTIVE}', '{State.UNIQUE_FROZEN}', '{State.UNIQUE_DETACHED}', '{State.ORPHANED}'),
                size UInt64)
                ENGINE {engine}
                ORDER BY (replica, state)
                SETTINGS storage_policy = '{storage_policy}'
        """,
        format_=None,
    )


def _get_table_function(
    ctx: Context,
    table: str,
    scope: Scope,
    cluster_name: Optional[str] = None,
) -> str:
    if scope == Scope.CLUSTER:
        assert cluster_name
        return f"cluster('{cluster_name}', {table})"
    elif scope == Scope.SHARD:
        return get_remote_table_for_hosts(ctx, table, ClickhouseInfo.get_replicas(ctx))

    return table


def _get_fill_local_blobs_table_query(
    ctx: Context,
    local_blobs_table: str,
    remote_data_paths_table: str,
    disk_name: str,
) -> Query:
    """
    Column 'size' in remote_data_paths is supported since 23.3.
    """
    settings = ""
    if match_ch_version(ctx, "24.3"):
        settings = "SETTINGS traverse_shadow_remote_data_paths=1"

    size = "size"
    if not match_ch_version(ctx, "23.3"):
        size = "0"

    user_password = clickhouse_client(ctx).password or ""

    query = f"""
        INSERT INTO {local_blobs_table}
            SELECT
                hostName(),
                remote_path,
                multiIf(
                    position(local_path, 'shadow/') > 0, '{LocalState.SHADOW}',
                    position(local_path, 'detached/') > 0, '{LocalState.DETACHED}',
                    '{LocalState.ACTIVE}'
                ) AS status,
                {size} AS obj_size,
                1 AS ref_count
                FROM {remote_data_paths_table}
                WHERE disk_name = '{disk_name}'
                {settings}
    """

    return Query(query, sensitive_args={"user_password": user_password})


def _get_fill_space_usage_query(
    ctx: Context,
    local_blobs_table: str,
    orphaned_blobs_table: str,
    space_usage_table_new: str,
) -> list[Query]:
    """
    CTE with INSERT supported since 25.3.
    """
    user_password = clickhouse_client(ctx).password or ""

    if match_ch_version(ctx, "25.3"):
        queries = [
            f"""
            WITH
            aggregated_blobs AS (
                SELECT
                    groupArraySorted(10)(replica) as replica,
                    obj_path,
                    state,
                    sum(obj_size) as sum_size,
                    sum(ref_count) as ref_count
                FROM {local_blobs_table}
                GROUP BY obj_path, state
            ),
            active AS (
                SELECT
                    replica,
                    sum(sum_size / ref_count) as size
                FROM aggregated_blobs
                WHERE state = '{LocalState.ACTIVE}'
                GROUP BY replica
            ),
            unique_frozen AS (
                SELECT
                    replica,
                    sum(sum_size / ref_count) as size
                FROM aggregated_blobs
                WHERE state = '{LocalState.SHADOW}'
                AND obj_path NOT IN (
                    SELECT obj_path FROM aggregated_blobs WHERE state = '{LocalState.ACTIVE}'
                )
                GROUP BY replica
            ),
            unique_detached AS (
                SELECT
                    replica,
                    sum(sum_size / ref_count) as size
                FROM aggregated_blobs
                WHERE state = '{LocalState.DETACHED}'
                AND obj_path NOT IN (
                    SELECT obj_path FROM aggregated_blobs WHERE state IN ('{LocalState.ACTIVE}', '{LocalState.SHADOW}')
                )
                GROUP BY replica
            )
            INSERT INTO {space_usage_table_new} (replica, state, size)
                SELECT replica, '{State.ACTIVE}', size FROM active
                UNION ALL
                SELECT replica, '{State.UNIQUE_FROZEN}', size FROM unique_frozen
                UNION ALL
                SELECT replica, '{State.UNIQUE_DETACHED}', size FROM unique_detached
                UNION ALL
                SELECT ['{UNKNOWN_REPLICAS_NAME}'], '{State.ORPHANED}', toFloat64(sum(obj_size)) FROM {orphaned_blobs_table}
        """
        ]
    else:
        query_active = f"""
            INSERT INTO {space_usage_table_new}
                SELECT
                    replica,
                    '{State.ACTIVE}',
                    sum(sum_size / ref_count)
                FROM (
                    SELECT
                        groupArraySorted(10)(replica) as replica,
                        obj_path,
                        state,
                        sum(obj_size) as sum_size,
                        sum(ref_count) as ref_count
                    FROM {local_blobs_table}
                    WHERE state = '{LocalState.ACTIVE}'
                    GROUP BY obj_path, state
                )
                GROUP BY replica
        """
        query_frozen = f"""
            INSERT INTO {space_usage_table_new}
                SELECT
                    replica,
                    '{State.UNIQUE_FROZEN}',
                    sum(sum_size / ref_count)
                FROM (
                    SELECT
                        groupArraySorted(10)(replica) as replica,
                        obj_path,
                        state,
                        sum(obj_size) as sum_size,
                        sum(ref_count) as ref_count
                    FROM {local_blobs_table}
                    WHERE state = '{LocalState.SHADOW}'
                    AND obj_path NOT IN (
                        SELECT obj_path FROM {local_blobs_table} WHERE state = '{LocalState.ACTIVE}'
                    )
                    GROUP BY obj_path, state
                )
                GROUP BY replica
        """
        query_detached = f"""
            INSERT INTO {space_usage_table_new}
                SELECT
                    replica,
                    '{State.UNIQUE_DETACHED}',
                    sum(sum_size / ref_count)
                FROM (
                    SELECT
                        groupArraySorted(10)(replica) as replica,
                        obj_path,
                        state,
                        sum(obj_size) as sum_size,
                        sum(ref_count) as ref_count
                    FROM {local_blobs_table}
                    WHERE state = '{LocalState.DETACHED}'
                    AND obj_path NOT IN (
                        SELECT obj_path FROM {local_blobs_table} WHERE state IN ('{LocalState.ACTIVE}', '{LocalState.SHADOW}')
                    )
                    GROUP BY obj_path, state
                )
                GROUP BY replica
        """
        query_orphaned = f"""
            INSERT INTO {space_usage_table_new}
                SELECT ['{UNKNOWN_REPLICAS_NAME}'], '{State.ORPHANED}', toFloat64(sum(obj_size)) FROM {orphaned_blobs_table}
        """

        queries = [query_active, query_frozen, query_detached, query_orphaned]

    return [
        Query(query, sensitive_args={"user_password": user_password})
        for query in queries
    ]


def _check_schema_mismatch_for_remote_blobs(create_query: str) -> bool:
    """
    Check if table schema is changed and table should be recreated
    """

    def _get_columns_dict(columns_str: str) -> dict[str, str]:
        res = {}
        columns_str = columns_str.strip("()")
        columns_list = [name_type.strip() for name_type in columns_str.split(",")]
        name_type_list = [name_type.split() for name_type in columns_list]

        for name_type_arr in name_type_list:
            name = name_type_arr[0].strip("`")
            c_type = name_type_arr[1]
            res[name] = c_type

        return res

    m = re.search(r"CREATE TABLE .* \((.*)\) ENGINE", create_query)
    logging.warning(m)
    if m:
        existing_columns = _get_columns_dict(m[1])
        new_columns = _get_columns_dict(REMOTE_BLOBS_COLUMNS)

        return existing_columns == new_columns

    raise RuntimeError(
        "Can't find columns in table create query for remote blobs table"
    )
