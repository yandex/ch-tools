import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional

from click import Context
from humanfriendly import format_size

from ch_tools.chadmin.internal.object_storage import cleanup_s3_object_storage
from ch_tools.chadmin.internal.object_storage.obj_list_item import ObjListItem
from ch_tools.chadmin.internal.object_storage.s3_cleanup import (
    ResultStatDict,
    StatisticsPartitioning,
    _init_key_in_stat,
)
from ch_tools.chadmin.internal.object_storage.s3_iterator import (
    s3_object_storage_iterator,
)
from ch_tools.chadmin.internal.system import match_ch_backup_version, match_ch_version
from ch_tools.chadmin.internal.utils import (
    DATETIME_FORMAT,
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
    stat_partitioning: StatisticsPartitioning = StatisticsPartitioning.ALL,
) -> ResultStatDict:
    """
    Clean orphaned S3 objects.
    """
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]
    config = ctx.obj["config"]["object_storage"]["clean"]

    listing_table = f"{config['listing_table_database']}.{config['listing_table_prefix']}{disk_conf.name}"
    orphaned_objects_table = f"{config['orphaned_objects_table_database']}.{config['orphaned_objects_table_prefix']}{disk_conf.name}"

    result_stat: ResultStatDict = {}
    _init_key_in_stat(result_stat, "Total")

    try:
        result_stat = _clean_object_storage(
            ctx,
            object_name_prefix,
            from_time,
            to_time,
            dry_run,
            use_saved_list,
            listing_table,
            orphaned_objects_table,
            verify_paths_regex,
            max_size_to_delete_bytes,
            max_size_to_delete_fraction,
            ignore_missing_cloud_storage_backups,
            stat_partitioning,
        )
    finally:
        if not keep_paths and not dry_run:
            _drop_table_on_shard(ctx, listing_table)
            _drop_table_on_shard(ctx, orphaned_objects_table)

    return result_stat


def collect_object_storage_info(
    ctx: Context,
    object_name_prefix: str,
    from_time: Optional[timedelta],
    to_time: timedelta,
) -> None:
    _list_local_blobs(ctx)
    _collect_orphaned_objects(
        ctx,
        object_name_prefix=object_name_prefix,
        from_time=from_time,
        to_time=to_time,
    )
    _collect_space_usage(ctx)


def get_object_storage_space_usage(
    ctx: Context,
    cluster_name: str,
    scope: Scope,
) -> dict:
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]
    config = ctx.obj["config"]["object_storage"]["space_usage"]
    space_usage_table = f"{config['space_usage_table_database']}.{config['space_usage_table_prefix']}{disk_conf.name}"

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


def _list_local_blobs(
    ctx: Context,
) -> None:
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]
    config = ctx.obj["config"]["object_storage"]["space_usage"]

    blob_state_table = f"{config['space_usage_table_database']}.{config['blob_state_table_prefix']}{disk_conf.name}"
    blob_state_table_zk_path_prefix = config["space_usage_table_zk_path_prefix"]
    storage_policy = config["storage_policy"]

    _create_local_object_listing_table(
        ctx,
        blob_state_table,
        blob_state_table_zk_path_prefix,
        storage_policy,
        drop_existing_table=True,
    )

    remote_data_paths_table = _get_table_function(
        ctx, REMOTE_DATA_PATHS_TABLE, Scope.SHARD, cluster_name=None
    )

    blob_state_query = _get_fill_blob_state_query(
        ctx, blob_state_table, remote_data_paths_table, disk_conf.name
    )

    timeout = ctx.obj["config"]["object_storage"]["clean"]["antijoin_timeout"]
    query_settings = {"receive_timeout": timeout, "max_execution_time": 0}
    execute_query(
        ctx,
        blob_state_query,
        timeout=timeout,
        settings=query_settings,
    )


def _collect_orphaned_objects(
    ctx: Context,
    object_name_prefix: str,
    from_time: Optional[timedelta],
    to_time: timedelta,
) -> None:
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]

    config = ctx.obj["config"]["object_storage"]["space_usage"]
    blob_state_table = f"{config['space_usage_table_database']}.{config['blob_state_table_prefix']}{disk_conf.name}"

    config = ctx.obj["config"]["object_storage"]["clean"]
    listing_table = f"{config['listing_table_database']}.{config['listing_table_prefix']}{disk_conf.name}"
    listing_table_zk_path_prefix = config["listing_table_zk_path_prefix"]
    orphaned_objects_table = f"{config['orphaned_objects_table_database']}.{config['orphaned_objects_table_prefix']}{disk_conf.name}"
    orphaned_objects_table_zk_path_prefix = config[
        "orphaned_objects_table_zk_path_prefix"
    ]
    storage_policy = config["storage_policy"]

    # Create listing table for storing paths from object storage.
    # Create orphaned objects for accumulating the result.
    _create_object_listing_table(
        ctx,
        listing_table,
        listing_table_zk_path_prefix,
        storage_policy,
        drop_existing_table=True,
    )
    _create_object_listing_table(
        ctx,
        orphaned_objects_table,
        orphaned_objects_table_zk_path_prefix,
        storage_policy,
        drop_existing_table=True,
    )

    prefix = object_name_prefix or disk_conf.prefix
    prefix = os.path.join(prefix, "")

    logging.info(
        f"Collecting objects... (Disk: '{disk_conf.name}', Endpoint '{disk_conf.endpoint_url}', Bucket: '{disk_conf.bucket_name}', Prefix: '{prefix}')",
    )
    _traverse_object_storage(ctx, listing_table, from_time, to_time, prefix)

    ch_client = clickhouse_client(ctx)
    user_password = ch_client.password or ""

    antijoin_query = Query(
        f"""
        INSERT INTO {orphaned_objects_table}
            SELECT last_modified, obj_path, obj_size FROM {listing_table} AS object_storage
            LEFT ANTI JOIN {blob_state_table} AS object_table
            ON object_table.obj_path = object_storage.obj_path
        """,
        sensitive_args={"user_password": user_password},
    )
    logging.info("Antijoin query: {}", str(antijoin_query))

    timeout = ctx.obj["config"]["object_storage"]["clean"]["antijoin_timeout"]
    query_settings = {"receive_timeout": timeout, "max_execution_time": 0}
    execute_query(
        ctx,
        antijoin_query,
        timeout=timeout,
        settings=query_settings,
    )


def _collect_space_usage(
    ctx: Context,
) -> None:
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]
    config = ctx.obj["config"]["object_storage"]["space_usage"]
    blob_state_table = f"{config['space_usage_table_database']}.{config['blob_state_table_prefix']}{disk_conf.name}"
    space_usage_table = f"{config['space_usage_table_database']}.{config['space_usage_table_prefix']}{disk_conf.name}"
    space_usage_table_new = f"{space_usage_table}_new"
    space_usage_table_zk_path_prefix = config["space_usage_table_zk_path_prefix"]

    config = ctx.obj["config"]["object_storage"]["clean"]
    orphaned_objects_table = f"{config['orphaned_objects_table_database']}.{config['orphaned_objects_table_prefix']}{disk_conf.name}"

    # This table will store the actual space usage data after the query
    _create_space_usage_table(
        ctx,
        space_usage_table_name=space_usage_table_new,
        replica_zk_prefix=space_usage_table_zk_path_prefix,
        storage_policy=config["storage_policy"],
        drop_existing_table=True,
    )
    # This table should always exist and store data from the previous run
    _create_space_usage_table(
        ctx,
        space_usage_table_name=space_usage_table,
        replica_zk_prefix=space_usage_table_zk_path_prefix,
        storage_policy=config["storage_policy"],
        drop_existing_table=False,
    )

    space_usage_query = _get_fill_space_usage_query(
        ctx, blob_state_table, orphaned_objects_table, space_usage_table_new
    )

    # TODO: should probably use different setting for timeout
    timeout = ctx.obj["config"]["object_storage"]["clean"]["antijoin_timeout"]
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
    _drop_table_on_shard(ctx, space_usage_table_new)


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
        SELECT last_modified, obj_path, obj_size FROM {{ table_name }}
        WHERE 1
        {% if from_time_cond %}
            AND last_modified >= toDateTime('{{ from_time_cond }}')
        {% endif %}
        {% if to_time_cond %}
            AND last_modified <= toDateTime('{{ to_time_cond }}')
        {% endif %}
        ORDER BY last_modified
        """

    def obj_list_iterator() -> Iterator[ObjListItem]:
        with execute_query(
            ctx,
            query,
            format_="TabSeparated",
            timeout=timeout,
            settings=query_settings,
            stream=True,
            table_name=table_name,
            from_time_cond=from_time_cond,
            to_time_cond=to_time_cond,
        ) as resp:
            for line in resp.iter_lines():
                yield ObjListItem.from_tab_separated(line.decode().strip())

    return obj_list_iterator


def _sanity_check_before_cleanup(
    ctx: Context,
    orphaned_objects_iterator: Callable,
    ch_client: ClickhouseClient,
    listing_size_in_bucket: int,
    blob_state_table: str,
    verify_paths_regex: Optional[str],
    dry_run: bool,
) -> None:

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
        ### Compare path from system.remote_data_path and from list to delete. They must match the regex.
        ### Perform such check to prevent silly mistakes if the paths are completely different.
        ch_objects_cnt = int(
            ch_client.query_json_data_first_row(
                query=Query(
                    f"SELECT count() FROM {blob_state_table}",
                    sensitive_args={"user_password": ch_client.password or ""},
                ),
                compact=True,
            )[0]
        )
        ## Nothing to check
        if ch_objects_cnt == 0:
            return
        path_form_ch = ch_client.query_json_data_first_row(
            query=Query(
                f"SELECT obj_path FROM {blob_state_table} LIMIT 1",
                sensitive_args={"user_password": ch_client.password or ""},
            ),
            compact=True,
        )[0]

        if not re.match(paths_regex, path_form_ch):
            raise_or_warn(
                dry_run,
                "Sanity check not passed, because obj_path({}) doesn't matches the regex({}).".format(
                    path_form_ch, paths_regex
                ),
            )

        for orphaned_object in orphaned_objects_iterator():
            if not re.match(paths_regex, orphaned_object.path):
                raise_or_warn(
                    dry_run,
                    "Sanity check not passed, because orphaned object({}) doesn't matches the regex({}).".format(
                        orphaned_object.path, paths_regex
                    ),
                )

    def perform_check_size() -> None:

        if listing_size_in_bucket == 0:
            return

        ### Total size of objects after cleanup must be very close to sum(bytes) FROM system.remote_data_paths
        size_to_delete = 0
        for orphaned_object in orphaned_objects_iterator():
            size_to_delete += orphaned_object.size

        if (
            listing_size_in_bucket * size_error_rate_threshold_fraction
            <= size_to_delete
        ):
            raise_or_warn(
                dry_run,
                "Potentially dangerous operation: Going to remove more than {}% of bucket content. To remove {}; listing size {}".format(
                    int(size_error_rate_threshold_fraction * 100),
                    format_size(size_to_delete),
                    format_size(listing_size_in_bucket),
                ),
            )

    perform_check_paths()

    ## In ver < 23.3 no column size in the system.remote_data_paths
    if match_ch_version(ctx, min_version="23.3"):
        perform_check_size()


def _clean_object_storage(
    ctx: Context,
    object_name_prefix: str,
    from_time: Optional[timedelta],
    to_time: timedelta,
    dry_run: bool,
    use_saved_list: bool,
    listing_table: str,
    orphaned_objects_table: str,
    verify_paths_regex: Optional[str] = None,
    max_size_to_delete_bytes: int = 0,
    max_size_to_delete_fraction: float = 1.0,
    ignore_missing_cloud_storage_backups: bool = False,
    stat_partitioning: StatisticsPartitioning = StatisticsPartitioning.ALL,
) -> ResultStatDict:
    """
    Delete orphaned objects from object storage.
    """
    result_stat: ResultStatDict = {}
    _init_key_in_stat(result_stat, "Total")

    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]
    ch_client = clickhouse_client(ctx)

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
        downloaded_backups = _download_missing_cloud_storage_backups(disk_conf.name)

    if use_saved_list:
        _list_local_blobs(ctx)
    else:
        collect_object_storage_info(ctx, object_name_prefix, from_time, to_time)

    if dry_run:
        logging.info("Counting orphaned objects...")
    else:
        logging.info("Deleting orphaned objects...")

    try:
        timeout = ctx.obj["config"]["object_storage"]["clean"]["antijoin_timeout"]
        query_settings = {"receive_timeout": timeout, "max_execution_time": 0}
        orphaned_objects_iterator = _object_list_generator(
            ctx,
            orphaned_objects_table,
            from_time=from_time if use_saved_list else None,
            to_time=to_time if use_saved_list else None,
            query_settings=query_settings,
            timeout=timeout,
        )
        listing_size_in_bucket = int(
            ch_client.query_json_data_first_row(
                query=f"SELECT sum(obj_size) FROM {listing_table}",
                compact=True,
            )[0]
        )

        config = ctx.obj["config"]["object_storage"]["space_usage"]
        blob_state_table = f"{config['space_usage_table_database']}.{config['blob_state_table_prefix']}{disk_conf.name}"

        if ctx.obj["config"]["object_storage"]["clean"]["verify"]:
            _sanity_check_before_cleanup(
                ctx,
                orphaned_objects_iterator,
                ch_client,
                listing_size_in_bucket,
                blob_state_table,
                verify_paths_regex,
                dry_run,
            )

        max_size_to_delete = listing_size_in_bucket * max_size_to_delete_fraction
        if max_size_to_delete_bytes:
            max_size_to_delete = min(max_size_to_delete, max_size_to_delete_bytes)

        for objects in chunked(orphaned_objects_iterator(), KEYS_BATCH_SIZE):
            batch_size = sum(obj.size for obj in objects)
            is_last_batch = False

            if result_stat["Total"]["total_size"] + batch_size > max_size_to_delete:
                # To fit into the size restriction: sort all objects by size
                # And remove elements from the end;
                is_last_batch = True
                objects = sorted(objects, key=lambda obj: obj.size)
                while (
                    result_stat["Total"]["total_size"] + batch_size > max_size_to_delete
                ):
                    batch_size -= objects[-1].size
                    objects.pop()

            cleanup_s3_object_storage(
                disk_conf, iter(objects), result_stat, stat_partitioning, dry_run
            )

            if is_last_batch:
                break

        deleted = result_stat["Total"]["deleted"]
        total_size = format_size(result_stat["Total"]["total_size"], binary=True)
        logging.info(
            f"{'Would delete' if dry_run else 'Deleted'} {deleted} objects with total size {total_size} from bucket [{disk_conf.bucket_name}]",
        )

    finally:
        _remove_backups_from_disk(disk_conf.name, downloaded_backups)

    return result_stat


def _traverse_object_storage(
    ctx: Context,
    listing_table: str,
    from_time: Optional[timedelta],
    to_time: timedelta,
    prefix: str,
) -> None:
    """
    Traverse S3 disk's bucket and put object names to the ClickHouse table.
    """
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
            _insert_listing_batch(ctx, obj_paths_batch, listing_table)
            obj_paths_batch.clear()

    # Insert the last batch (might be shorter)
    if obj_paths_batch:
        _insert_listing_batch(ctx, obj_paths_batch, listing_table)

    logging.info("Collected {} objects", counter)


def _insert_listing_batch(
    ctx: Context, obj_paths_batch: List[ObjListItem], listing_table: str
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
        f"INSERT INTO {listing_table} (last_modified, obj_path, obj_size) VALUES {batch_values}",
        format_=None,
    )


def _drop_table_on_shard(ctx: Context, table_name: str) -> None:
    execute_query_on_shard(ctx, f"DROP TABLE IF EXISTS {table_name} SYNC", format_=None)


def _create_object_listing_table(
    ctx: Context,
    table_name: str,
    replica_zk_prefix: str,
    storage_policy: str,
    drop_existing_table: bool,
) -> None:
    if drop_existing_table:
        _drop_table_on_shard(ctx, table_name)

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
        CREATE TABLE IF NOT EXISTS {table_name}
            (last_modified DateTime, obj_path String, obj_size UInt64)
            ENGINE {engine}
            ORDER BY (last_modified, obj_path)
            SETTINGS storage_policy = '{storage_policy}'
        """,
        format_=None,
    )


def _create_local_object_listing_table(
    ctx: Context,
    table_name: str,
    replica_zk_prefix: str,
    storage_policy: str,
    drop_existing_table: bool,
) -> None:
    if drop_existing_table:
        _drop_table_on_shard(ctx, table_name)

    engine = (
        "ReplicatedSummingMergeTree"
        + ZOOKEEPER_ARGS.format(
            replica_zk_prefix=replica_zk_prefix, table_name=table_name
        )
        if has_zk()
        else "MergeTree"
    )

    execute_query_on_shard(
        ctx,
        f"""
            CREATE TABLE IF NOT EXISTS {table_name}
                (obj_path String, state String, obj_size UInt64, ref_count UInt32)
                ENGINE {engine}
                ORDER BY (obj_path, state)
                SETTINGS storage_policy = '{storage_policy}'
        """,
        format_=None,
    )


def _create_space_usage_table(
    ctx: Context,
    space_usage_table_name: str,
    replica_zk_prefix: str,
    storage_policy: str,
    drop_existing_table: bool,
) -> None:
    if drop_existing_table:
        _drop_table_on_shard(ctx, space_usage_table_name)

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


def _get_fill_blob_state_query(
    ctx: Context,
    blob_state_table: str,
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
        INSERT INTO {blob_state_table}
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
    blob_state_table: str,
    orphaned_objects_table: str,
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
                        FROM {blob_state_table}
                        WHERE state = 'active'
                        GROUP BY obj_path)
                ),
            unique_frozen AS
                (SELECT sum(shadow.obj_size / shadow.ref_count) AS size
                    FROM {blob_state_table} AS shadow
                    LEFT ANTI JOIN
                    (
                        SELECT DISTINCT obj_path
                        FROM {blob_state_table}
                        WHERE state = 'active'
                    ) AS has_other
                        ON shadow.obj_path = has_other.obj_path
                        WHERE
                            shadow.state = 'shadow'
                ),
            unique_detached AS
                (SELECT sum(detached.obj_size / detached.ref_count) AS size
                    FROM {blob_state_table} AS detached
                    LEFT ANTI JOIN
                    (
                        SELECT DISTINCT obj_path
                        FROM {blob_state_table}
                        WHERE state IN ('active', 'shadow')
                    ) AS has_other
                        ON detached.obj_path = has_other.obj_path
                        WHERE
                            detached.state = 'detached'
                ),
            orphaned AS
                (SELECT sum(obj_size) AS size
                    FROM {orphaned_objects_table}
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
                            FROM {blob_state_table}
                            WHERE state = 'active'
                            GROUP BY obj_path)
                    ) AS active,
                    (SELECT sum(shadow.obj_size / shadow.ref_count)
                        FROM {blob_state_table} AS shadow
                        LEFT ANTI JOIN
                        (
                            SELECT DISTINCT obj_path
                            FROM {blob_state_table}
                            WHERE state = 'active'
                        ) AS has_other
                            ON shadow.obj_path = has_other.obj_path
                            WHERE
                                shadow.state = 'shadow'
                    ) AS unique_frozen,
                    (SELECT sum(detached.obj_size / detached.ref_count)
                        FROM {blob_state_table} AS detached
                        LEFT ANTI JOIN
                        (
                            SELECT DISTINCT obj_path
                            FROM {blob_state_table}
                            WHERE state IN ('active', 'shadow')
                        ) AS has_other
                            ON detached.obj_path = has_other.obj_path
                            WHERE
                                detached.state = 'detached'
                    ) AS unique_detached,
                    (SELECT sum(obj_size)FROM {orphaned_objects_table}) AS orphaned
        """

    return Query(
        query,
        sensitive_args={"user_password": user_password},
    )
