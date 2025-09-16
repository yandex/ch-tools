import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

import click
from click import Context
from humanfriendly import format_size

from ch_tools.chadmin.internal.object_storage import cleanup_s3_object_storage
from ch_tools.chadmin.internal.object_storage.obj_list_item import ObjListItem
from ch_tools.chadmin.internal.object_storage.s3_iterator import (
    s3_object_storage_iterator,
)
from ch_tools.chadmin.internal.system import match_ch_backup_version, match_ch_version
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


class CleanScope(str, Enum):
    """
    Define a local metadata search scope to clean.
    """

    HOST = "host"
    SHARD = "shard"
    CLUSTER = "cluster"


def _create_object_listing_table(
    ctx: Context,
    table_name: str,
    replica_zk_prefix: str,
    storage_policy: str,
    recreate_table: bool,
) -> None:
    if not recreate_table:
        _drop_table_on_shard(ctx, table_name)

    _create_table_on_shard(ctx, table_name, replica_zk_prefix, storage_policy)


def clean(
    ctx: Context,
    object_name_prefix: str,
    from_time: Optional[timedelta],
    to_time: timedelta,
    clean_scope: CleanScope,
    cluster_name: str,
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
    if from_time is not None and to_time < from_time:
        raise click.BadParameter(
            "'from_time' parameter must be greater than 'to_time'",
            param_hint="--from-time",
        )

    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]
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
    try:
        _create_object_listing_table(
            ctx,
            listing_table,
            listing_table_zk_path_prefix,
            storage_policy,
            use_saved_list,
        )
        _create_object_listing_table(
            ctx,
            orphaned_objects_table,
            orphaned_objects_table_zk_path_prefix,
            storage_policy,
            False,
        )

        deleted, total_size = _clean_object_storage(
            ctx,
            object_name_prefix,
            from_time,
            to_time,
            clean_scope,
            cluster_name,
            dry_run,
            listing_table,
            orphaned_objects_table,
            use_saved_list,
            verify_paths_regex,
            max_size_to_delete_bytes,
            max_size_to_delete_fraction,
            ignore_missing_cloud_storage_backups,
        )
    finally:
        if not keep_paths:
            _drop_table_on_shard(ctx, listing_table)
            _drop_table_on_shard(ctx, orphaned_objects_table)

    return deleted, total_size


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
    remote_data_paths_table: str,
    verify_paths_regex: Optional[str],
    clean_scope: CleanScope,
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
        ][clean_scope]

    remote_data_paths_query_settings = (
        {"traverse_shadow_remote_data_paths": 1}
        if match_ch_version(ctx, min_version="24.3")
        else {}
    )

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
                    f"SELECT count() FROM {remote_data_paths_table}",
                    sensitive_args={"user_password": ch_client.password or ""},
                ),
                settings=remote_data_paths_query_settings,
                compact=True,
            )[0]
        )
        ## Nothing to check
        if ch_objects_cnt == 0:
            return
        path_form_ch = ch_client.query_json_data_first_row(
            query=Query(
                f"SELECT remote_path FROM {remote_data_paths_table} LIMIT 1",
                sensitive_args={"user_password": ch_client.password or ""},
            ),
            settings=remote_data_paths_query_settings,
            compact=True,
        )[0]

        if not re.match(paths_regex, path_form_ch):
            raise_or_warn(
                dry_run,
                "Sanity check not passed, because remote_path({}) doesn't matches the regex({}).".format(
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
                    format_size(listing_size_in_bucket),
                    format_size(size_to_delete),
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
    clean_scope: CleanScope,
    cluster_name: str,
    dry_run: bool,
    listing_table: str,
    orphaned_objects_table: str,
    use_saved_list: bool,
    verify_paths_regex: Optional[str] = None,
    max_size_to_delete_bytes: int = 0,
    max_size_to_delete_fraction: float = 1.0,
    ignore_missing_cloud_storage_backups: bool = False,
) -> Tuple[int, int]:
    """
    Delete orphaned objects from object storage.
    """
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]
    prefix = object_name_prefix or _get_default_object_name_prefix(
        clean_scope, disk_conf
    )
    prefix = os.path.join(prefix, "")

    if not use_saved_list:
        logging.info(
            f"Collecting objects... (Disk: '{disk_conf.name}', Endpoint '{disk_conf.endpoint_url}', Bucket: '{disk_conf.bucket_name}', Prefix: '{prefix}')",
        )
        _traverse_object_storage(ctx, listing_table, from_time, to_time, prefix)

    ch_client = clickhouse_client(ctx)
    user_name = ch_client.user or ""
    user_password = ch_client.password or ""

    remote_data_paths_table = "system.remote_data_paths"

    if clean_scope == CleanScope.CLUSTER:
        remote_data_paths_table = (
            f"clusterAllReplicas('{cluster_name}', {remote_data_paths_table})"
        )
    elif clean_scope == CleanScope.SHARD:
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
        remote_data_paths_table = f"{remote_clause}('{replicas}', {remote_data_paths_table}, '{user_name}', '{{user_password}}')"

    settings = ""
    if match_ch_version(ctx, min_version="24.3"):
        settings = "SETTINGS traverse_shadow_remote_data_paths=1"

    antijoin_query = Query(
        f"""
        INSERT INTO {orphaned_objects_table}
            SELECT obj_path, obj_size FROM {listing_table} AS object_storage
            LEFT ANTI JOIN {remote_data_paths_table} AS object_table
            ON object_table.remote_path = object_storage.obj_path
                    AND object_table.disk_name = '{disk_conf.name}'
        {settings}
    """,
        sensitive_args={"user_password": user_password},
    )
    logging.info("Antijoin query: {}", str(antijoin_query))

    downloaded_backups = []
    if (
        not ignore_missing_cloud_storage_backups
        and match_ch_backup_version("2.641.197281242")
        and match_ch_version(ctx, min_version="24.3")
    ):
        logging.info(
            "Will download cloud storage metadata from backups to shadow directory if they are missing"
        )
        downloaded_backups = _download_missing_cloud_storage_backups(disk_conf.name)

    if dry_run:
        logging.info("Counting orphaned objects...")
    else:
        logging.info("Deleting orphaned objects...")

    try:
        timeout = ctx.obj["config"]["object_storage"]["clean"]["antijoin_timeout"]
        query_settings = {"receive_timeout": timeout, "max_execution_time": 0}
        execute_query(
            ctx,
            antijoin_query,
            timeout=timeout,
            settings=query_settings,
        )
        orphaned_objects_iterator = _object_list_generator(
            ch_client, orphaned_objects_table, query_settings, timeout
        )
        listing_size_in_bucket = int(
            ch_client.query_json_data_first_row(
                query=f"SELECT sum(obj_size) FROM {listing_table}",
                compact=True,
            )[0]
        )

        if ctx.obj["config"]["object_storage"]["clean"]["verify"]:
            _sanity_check_before_cleanup(
                ctx,
                orphaned_objects_iterator,
                ch_client,
                listing_size_in_bucket,
                remote_data_paths_table,
                verify_paths_regex,
                clean_scope,
                dry_run,
            )

        deleted, total_size = 0, 0

        max_size_to_delete = listing_size_in_bucket * max_size_to_delete_fraction
        if max_size_to_delete_bytes:
            max_size_to_delete = min(max_size_to_delete, max_size_to_delete_bytes)

        for objects in chunked(orphaned_objects_iterator(), KEYS_BATCH_SIZE):
            batch_size = sum(obj.size for obj in objects)
            is_last_batch = False

            if total_size + batch_size > max_size_to_delete:
                # To fit into the size restriction: sort all objects by size
                # And remove elements from the end;
                is_last_batch = True
                objects = sorted(objects, key=lambda obj: obj.size)
                while total_size + batch_size > max_size_to_delete:
                    batch_size -= objects[-1].size
                    objects.pop()

            chunk_deleted, chunk_size = cleanup_s3_object_storage(
                disk_conf, iter(objects), dry_run
            )
            deleted += chunk_deleted
            total_size += chunk_size

            if is_last_batch:
                break

        logging.info(
            f"{'Would delete' if dry_run else 'Deleted'} {deleted} objects with total size {format_size(total_size, binary=True)} from bucket [{disk_conf.bucket_name}] with prefix {prefix}",
        )
    finally:
        _remove_backups_from_disk(disk_conf.name, downloaded_backups)

    return deleted, total_size


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

        obj_paths_batch.append(ObjListItem(obj.key, obj.size))
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
    batch_values = ",".join(f"('{item.path}',{item.size})" for item in obj_paths_batch)
    execute_query(
        ctx,
        f"INSERT INTO {listing_table} (obj_path, obj_size) VALUES {batch_values}",
        format_=None,
    )


def _drop_table_on_shard(ctx: Context, table_name: str) -> None:
    execute_query_on_shard(ctx, f"DROP TABLE IF EXISTS {table_name} SYNC", format_=None)


def _create_table_on_shard(
    ctx: Context, table_name: str, table_zk_path_prefix: str, storage_policy: str
) -> None:
    engine = (
        f"ReplicatedMergeTree('{table_zk_path_prefix}/{{shard}}/{table_name}', '{{replica}}')"
        if has_zk()
        else "MergeTree"
    )

    execute_query_on_shard(
        ctx,
        f"CREATE TABLE IF NOT EXISTS {table_name} (obj_path String, obj_size UInt64) ENGINE {engine} ORDER BY obj_path SETTINGS storage_policy = '{storage_policy}'",
        format_=None,
    )


def _get_default_object_name_prefix(
    clean_scope: CleanScope, disk_conf: S3DiskConfiguration
) -> str:
    """
    Returns default object name prefix for object storage.

    Keep trailing '/'.
    """
    if clean_scope == CleanScope.CLUSTER:
        # NOTE: Ya.Cloud specific code
        # Remove the last "shard" component of the path.
        # In general case define `--object-name-prefix` explicitly
        cluster_path = os.path.split(disk_conf.prefix.rstrip("/"))[0]
        return cluster_path + ("/" if cluster_path else "")

    return disk_conf.prefix


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
