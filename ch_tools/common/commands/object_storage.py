import os
import re
import subprocess
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, Generator, Iterator, List, Optional, Tuple

from click import Context
from humanfriendly import format_size

from ch_tools.chadmin.internal.object_storage import cleanup_s3_object_storage
from ch_tools.chadmin.internal.object_storage.obj_list_item import ObjListItem
from ch_tools.chadmin.internal.object_storage.s3_iterator import (
    s3_object_storage_iterator,
)
from ch_tools.chadmin.internal.system import match_ch_backup_version, match_ch_version
from ch_tools.chadmin.internal.table import (
    drop_table,
    drop_table_on_shard,
    list_tables,
    table_exists,
)
from ch_tools.chadmin.internal.utils import (
    chunked,
    execute_query,
    execute_query_on_shard,
    remove_from_disk,
)
from ch_tools.chadmin.internal.zookeeper import has_zk
from ch_tools.common import logging
from ch_tools.common.backup import CHS3_BACKUPS_DIRECTORY, get_missing_chs3_backups
from ch_tools.common.clickhouse.client.clickhouse_client import (
    ClickhouseClient,
    clickhouse_client,
)
from ch_tools.common.clickhouse.client.query import Query
from ch_tools.common.clickhouse.config.clickhouse import ClickhousePort
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
# Timestamp format for parsing table timestamps
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"


class Scope(str, Enum):
    """
    Define a local metadata search scope.
    """

    HOST = "host"
    SHARD = "shard"
    CLUSTER = "cluster"


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
) -> Tuple[int, int]:
    """
    Clean orphaned S3 objects.
    """
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]
    config = ctx.obj["config"]["object_storage"]["space_usage"]
    ch_client = clickhouse_client(ctx)

    _cleanup_old_service_tables(ctx)

    downloaded_backups = _download_backups_on_disk(
        ctx, disk_conf.name, ignore_missing_cloud_storage_backups
    )

    if dry_run:
        logging.info("Counting orphaned objects...")
    else:
        logging.info("Deleting orphaned objects...")

    try:
        with (
            _remote_blobs_table(
                ctx,
                object_name_prefix,
                from_time,
                to_time,
                unique_name=not keep_paths,
                keep_table=keep_paths,
                use_saved=use_saved_list,
            ) as remote_blobs_table,
            _local_blobs_table(ctx) as local_blobs_table,
            _orphaned_blobs_table(
                ctx, local_blobs_table, remote_blobs_table
            ) as orphaned_blobs_table,
        ):
            timeout = config["antijoin_timeout"]
            query_settings = {"receive_timeout": timeout, "max_execution_time": 0}
            orphaned_objects_iterator = _object_list_generator(
                ch_client, orphaned_blobs_table, query_settings, timeout
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

            deleted, total_size = _cleanup_orphaned_objects(
                orphaned_objects_iterator,
                disk_conf,
                listing_size_in_bucket,
                max_size_to_delete_bytes,
                max_size_to_delete_fraction,
                dry_run,
            )

    finally:
        _remove_backups_from_disk(disk_conf.name, downloaded_backups)

    return deleted, total_size


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

    with (
        _remote_blobs_table(
            ctx,
            object_name_prefix,
            from_time,
            to_time,
            unique_name=False,
            keep_table=True,
            use_saved=use_saved,
        ) as remote_blobs_table,
        _local_blobs_table(ctx, unique_name=True) as local_blobs_table,
        _orphaned_blobs_table(
            ctx, local_blobs_table, remote_blobs_table, unique_name=True
        ) as orphaned_blobs_table,
    ):
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
        f"""
            SELECT
                sum(active) AS active,
                sum(unique_frozen) AS unique_frozen,
                sum(unique_detached) AS unique_detached,
                sum(orphaned) AS orphaned
            FROM {space_usage_table}
        """
    )

    res = execute_query(
        ctx,
        parts_usage_query,
        format_="JSON",
    )[
        "data"
    ][0]

    return res


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
    total_size: int,
    max_size_to_delete: int,
) -> Tuple[List[ObjListItem], bool]:
    """
    Processes a batch of objects, fitting them to size constraints.
    Returns (processed_objects, is_last_batch).
    """
    batch_size = sum(obj.size for obj in objects)
    is_last_batch = False

    if total_size + batch_size > max_size_to_delete:
        # To fit into the size restriction: sort all objects by size
        # And remove elements from the end
        is_last_batch = True
        objects = sorted(objects, key=lambda obj: obj.size)
        while total_size + batch_size > max_size_to_delete:
            batch_size -= objects[-1].size
            objects.pop()

    return objects, is_last_batch


def _cleanup_orphaned_objects(
    orphaned_objects_iterator: Callable,
    disk_conf: S3DiskConfiguration,
    listing_size_in_bucket: int,
    max_size_to_delete_bytes: int,
    max_size_to_delete_fraction: float,
    dry_run: bool,
) -> Tuple[int, int]:
    """
    Performs the main logic for cleaning up orphaned objects.
    """
    deleted, total_size = 0, 0

    max_size_to_delete = _calculate_size_limits(
        listing_size_in_bucket, max_size_to_delete_bytes, max_size_to_delete_fraction
    )

    for objects in chunked(orphaned_objects_iterator(), KEYS_BATCH_SIZE):
        objects, is_last_batch = _process_objects_batch(
            objects, total_size, max_size_to_delete
        )

        chunk_deleted, chunk_size = cleanup_s3_object_storage(
            disk_conf, iter(objects), dry_run
        )
        deleted += chunk_deleted
        total_size += chunk_size

        if is_last_batch:
            break

    logging.info(
        f"{'Would delete' if dry_run else 'Deleted'} {deleted} objects with total size {format_size(total_size, binary=True)} from bucket [{disk_conf.bucket_name}]",
    )

    return deleted, total_size


def _cleanup_old_service_tables(ctx: Context) -> None:
    """
    Remove old service tables (local_blobs, orphaned_blobs, remote_blobs) that have unique names
    (contain UUID and timestamp) and are older than configured retention period.
    This helps prevent accumulation of temporary tables from previous runs.
    """
    config = ctx.obj["config"]["object_storage"]["space_usage"]
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]

    # Get retention period from config (default to 7 days if not specified)
    retention_days = config.get("service_tables_retention_days", 7)
    retention_threshold = datetime.now() - timedelta(days=retention_days)

    # Table prefixes to clean up
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

            # Get tables matching the prefix pattern
            tables = list_tables(
                ctx, database_name=database, table_pattern=table_pattern
            )

            for table_info in tables:
                table_name = table_info["name"]

                if match := re.search(UNIQUE_TABLE_SUFFIX_PATTERN, table_name):
                    table_timestamp_str = match.group(1)
                    try:
                        table_timestamp = datetime.strptime(
                            table_timestamp_str, TIMESTAMP_FORMAT
                        )

                        # Delete table if it's older than retention period
                        if table_timestamp < retention_threshold:
                            full_table_name = f"{database}.{table_name}"
                            logging.info(
                                f"Removing old service table: {full_table_name}"
                            )

                            # Use appropriate drop method based on table type
                            if table_prefix == config["remote_blobs_table_prefix"]:
                                drop_table_on_shard(ctx, full_table_name)
                            else:
                                drop_table(ctx, full_table_name)

                    except ValueError:
                        # Skip tables with invalid timestamp format
                        logging.warning(
                            f"Skipping table with invalid timestamp format: {table_name}"
                        )
                        continue

        except Exception as e:
            # Log error but don't fail the entire operation
            logging.warning(f"Error cleaning up old {table_prefix} tables: {e}")


def _get_table_name(ctx: Context, table_prefix: str, unique_name: bool = False) -> str:
    """
    Generates table name with UUID+timestamp if unique_name=True.
    """
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]
    config = ctx.obj["config"]["object_storage"]["space_usage"]

    base_name = f"{config['database']}.{table_prefix}{disk_conf.name}"

    if unique_name:
        table_uuid = str(uuid.uuid4()).replace("-", "_")
        timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)
        return f"{base_name}_{table_uuid}_{timestamp}"
    else:
        return base_name


@contextmanager
def _local_blobs_table(
    ctx: Context,
    unique_name: bool = True,
    keep_table: bool = True,
    use_saved: bool = False,
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

        yield local_blobs_table
    finally:
        if not keep_table:
            drop_table(ctx, local_blobs_table)


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
            drop_table(ctx, orphaned_blobs_table)


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

    try:
        if use_saved:
            table_name_only = remote_blobs_table.split(".")[1]
            if not table_exists(ctx, config["database"], table_name_only):
                raise RuntimeError(
                    f"Can't use saved {remote_blobs_table} because it does not exist"
                )
        else:
            _create_remote_blobs_table(
                ctx,
                remote_blobs_table,
                config["zk_path_prefix"],
                config["storage_policy"],
                drop_existing_table=True,
            )
            _collect_remote_blobs(
                ctx, from_time, to_time, object_name_prefix, remote_blobs_table
            )

        yield remote_blobs_table
    finally:
        if not keep_table:
            drop_table_on_shard(ctx, remote_blobs_table)


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
            SELECT obj_path, obj_size FROM {remote_blobs_table} AS object_storage
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

    space_usage_query = _get_fill_space_usage_query(
        ctx, local_blobs_table, orphaned_blobs_table, space_usage_table_new
    )

    # TODO: should probably use different setting for timeout
    timeout = config["antijoin_timeout"]
    query_settings = {"receive_timeout": timeout, "max_execution_time": 0}
    execute_query(
        ctx,
        space_usage_query,
        timeout=timeout,
        settings=query_settings,
    )

    execute_query(ctx, f"TRUNCATE TABLE {space_usage_table} SYNC")
    execute_query(
        ctx, f"INSERT INTO {space_usage_table} SELECT * FROM {space_usage_table_new}"
    )
    drop_table_on_shard(ctx, space_usage_table_new)


def _object_list_generator(
    ch_client: ClickhouseClient,
    table_name: str,
    query_settings: Dict[str, Any],
    timeout: Optional[int] = None,
) -> Callable:

    def obj_list_iterator() -> Iterator[ObjListItem]:
        with ch_client.query(
            f"SELECT obj_path, obj_size FROM {table_name}",
            format_="TabSeparated",
            timeout=timeout,
            settings=query_settings,
            stream=True,
        ) as resp:
            for line in resp.iter_lines():
                yield ObjListItem.from_tab_separated(line.decode().strip())

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

    def raise_or_warn(dry_run: bool, msg: str) -> None:
        if not dry_run:
            raise RuntimeError(msg)

        logging.warning(f"Warning: {msg}")

    def perform_check_paths() -> None:
        """
        Validate that object paths match expected regex pattern.
        This prevents accidental deletion of objects with unexpected paths.
        """
        # Get count of orphaned objects to validate
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

        # Validate sample path against regex
        if not re.match(paths_regex, sample_path):
            raise_or_warn(
                dry_run,
                f"Path validation failed: sample path '{sample_path}' doesn't match regex '{paths_regex}'",
            )

        # Validate all paths from iterator
        for orphaned_object in orphaned_objects_iterator():
            if not re.match(paths_regex, orphaned_object.path):
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

    # Execute validation checks
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

        obj_paths_batch.append(ObjListItem(obj.key, obj.size))
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
    batch_values = ",".join(f"('{item.path}',{item.size})" for item in obj_paths_batch)
    execute_query(
        ctx,
        f"INSERT INTO {remote_blobs_table} (obj_path, obj_size) VALUES {batch_values}",
        format_=None,
    )


def _create_remote_blobs_table(
    ctx: Context,
    table_name: str,
    replica_zk_prefix: str,
    storage_policy: str,
    drop_existing_table: bool = False,
) -> None:
    if drop_existing_table:
        drop_table_on_shard(ctx, table_name)

    engine = (
        "ReplicatedMergeTree"
        + ZOOKEEPER_ARGS.format(
            replica_zk_prefix=replica_zk_prefix, table_name=table_name
        )
        if has_zk()
        else "MergeTree"
    )

    execute_query_on_shard(
        ctx,
        f"""
          CREATE TABLE IF NOT EXISTS {table_name} (obj_path String, obj_size UInt64) ENGINE {engine}
            ORDER BY obj_path SETTINGS storage_policy = '{storage_policy}'
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
        drop_table(ctx, table_name)

    execute_query(
        ctx,
        f"""
            CREATE TABLE IF NOT EXISTS {table_name}
                (obj_path String, state String, obj_size UInt64, ref_count UInt32)
                ENGINE SummingMergeTree
                ORDER BY (obj_path, state)
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
        drop_table(ctx, table_name)

    execute_query(
        ctx,
        f"""
            CREATE TABLE IF NOT EXISTS {table_name}
                (obj_path String, obj_size UInt64)
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
        drop_table_on_shard(ctx, space_usage_table_name)

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
                (active UInt64, unique_frozen UInt64, unique_detached UInt64, orphaned UInt64)
                ENGINE {engine}
                ORDER BY active
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
    ch_client = clickhouse_client(ctx)
    user_name = ch_client.user or ""

    result = table

    if scope == Scope.CLUSTER:
        assert cluster_name
        result = f"cluster('{cluster_name}', {table})"
    elif scope == Scope.SHARD:
        #  It is believed that all hosts in shard have the same port set, so check current for tcp port
        if ch_client.check_port(ClickhousePort.TCP_SECURE):
            remote_clause = "remoteSecure"
        elif ch_client.check_port(ClickhousePort.TCP):
            remote_clause = "remote"
        else:
            raise RuntimeError(
                "For using remote() table function tcp port must be defined"
            )

        replicas = ",".join(ClickhouseInfo.get_replicas(ctx))
        result = f"{remote_clause}('{replicas}', {table}, '{user_name}', '{{user_password}}')"

    return result


def _download_missing_cloud_storage_backups(
    disk: str,
) -> list[str]:
    """
    Downloads created cloud storage backups if they are missing in shadow directory.
    """
    missing_backups = get_missing_chs3_backups(disk)

    for backup in missing_backups:
        logging.info(f"Downloading cloud storage metadata from '{backup}'")

        cmd = ["ch-backup", "get-cloud-storage-metadata", "--disk", disk, backup]
        proc = subprocess.run(
            cmd,
            shell=False,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        if proc.returncode:
            _remove_backups_from_disk(disk, missing_backups)
            raise RuntimeError(
                f"Downloading cloud storage metadata command has failed: retcode {proc.returncode}, stderr: {proc.stderr.decode()}"
            )

    return missing_backups


def _download_backups_on_disk(
    ctx: Context, disk_name: str, ignore_missing_cloud_storage_backups: bool
) -> List[str]:
    """
    Prepares cloud storage backups by downloading missing metadata.
    """
    downloaded_backups = []
    # Setting traverse_shadow_remote_data_paths is available since ClickHouse 24.3
    # Download metadata command is available since ch-backup 2.641.197281242
    if (
        not ignore_missing_cloud_storage_backups
        and match_ch_backup_version("2.641.197281242")
        and match_ch_version(ctx, min_version="24.3")
    ):
        logging.info(
            "Will download cloud storage metadata from backups to shadow directory if they are missing"
        )
        downloaded_backups = _download_missing_cloud_storage_backups(disk_name)

    return downloaded_backups


def _remove_backups_from_disk(
    disk: str,
    backups: list[str],
) -> None:
    """
    Removes backups from disk. Should be used to clean missing cloud storage backups after deleting orphaned objects.
    """
    backups_path = CHS3_BACKUPS_DIRECTORY.format(disk=disk)
    for backup in backups:
        remove_from_disk(os.path.join(backups_path, backup))


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
                remote_path,
                multiIf(
                    position(local_path, 'shadow/') > 0, 'shadow',
                    position(local_path, 'detached/') > 0, 'detached',
                    'active'
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
) -> Query:
    """
    CTE with INSERT supported since 25.3.
    """
    user_password = clickhouse_client(ctx).password or ""

    if match_ch_version(ctx, "25.3"):
        query = f"""
            WITH active AS
                (SELECT sum(size / count) as size
                    FROM (SELECT sum(obj_size) size, sum(ref_count) count
                        FROM {local_blobs_table}
                        WHERE state = 'active'
                        GROUP BY obj_path)
                ),
            unique_frozen AS
                (SELECT sum(shadow.obj_size / shadow.ref_count) AS size
                    FROM {local_blobs_table} AS shadow
                    LEFT ANTI JOIN
                    (
                        SELECT DISTINCT obj_path
                        FROM {local_blobs_table}
                        WHERE state = 'active'
                    ) AS has_other
                        ON shadow.obj_path = has_other.obj_path
                        WHERE
                            shadow.state = 'shadow'
                ),
            unique_detached AS
                (SELECT sum(detached.obj_size / detached.ref_count) AS size
                    FROM {local_blobs_table} AS detached
                    LEFT ANTI JOIN
                    (
                        SELECT DISTINCT obj_path
                        FROM {local_blobs_table}
                        WHERE state IN ('active', 'shadow')
                    ) AS has_other
                        ON detached.obj_path = has_other.obj_path
                        WHERE
                            detached.state = 'detached'
                ),
            orphaned AS
                (SELECT sum(obj_size) AS size
                    FROM {orphaned_blobs_table}
                )
            INSERT INTO {space_usage_table_new}
                SELECT
                    (SELECT size FROM active),
                    (SELECT size FROM unique_frozen),
                    (SELECT size FROM unique_detached),
                    (SELECT size FROM orphaned)
        """
    else:
        query = f"""
            INSERT INTO {space_usage_table_new}
                SELECT
                    (SELECT sum(size / count)
                        FROM (SELECT sum(obj_size) size, sum(ref_count) count
                            FROM {local_blobs_table}
                            WHERE state = 'active'
                            GROUP BY obj_path)
                    ) AS active,
                    (SELECT sum(shadow.obj_size / shadow.ref_count)
                        FROM {local_blobs_table} AS shadow
                        LEFT ANTI JOIN
                        (
                            SELECT DISTINCT obj_path
                            FROM {local_blobs_table}
                            WHERE state = 'active'
                        ) AS has_other
                            ON shadow.obj_path = has_other.obj_path
                            WHERE
                                shadow.state = 'shadow'
                    ) AS unique_frozen,
                    (SELECT sum(detached.obj_size / detached.ref_count)
                        FROM {local_blobs_table} AS detached
                        LEFT ANTI JOIN
                        (
                            SELECT DISTINCT obj_path
                            FROM {local_blobs_table}
                            WHERE state IN ('active', 'shadow')
                        ) AS has_other
                            ON detached.obj_path = has_other.obj_path
                            WHERE
                                detached.state = 'detached'
                    ) AS unique_detached,
                    (SELECT sum(obj_size)FROM {orphaned_blobs_table}) AS orphaned
        """

    return Query(
        query,
        sensitive_args={"user_password": user_password},
    )
