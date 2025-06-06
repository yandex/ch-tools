import os
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import List, Optional, Tuple

import click
from click import Context
from humanfriendly import format_size

from ch_tools.chadmin.internal.object_storage import (
    ObjListItem,
    cleanup_s3_object_storage,
)
from ch_tools.chadmin.internal.object_storage.s3_iterator import (
    s3_object_storage_iterator,
)
from ch_tools.chadmin.internal.system import match_ch_version
from ch_tools.chadmin.internal.utils import (
    chunked,
    execute_query,
    execute_query_on_shard,
)
from ch_tools.chadmin.internal.zookeeper import has_zk
from ch_tools.common import logging
from ch_tools.common.clickhouse.client.clickhouse_client import clickhouse_client
from ch_tools.common.clickhouse.client.query import Query
from ch_tools.common.clickhouse.config import get_cluster_name
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
KEYS_BATCH_SIZE = 100_000


class CleanScope(str, Enum):
    """
    Define a local metadata search scope to clean.
    """

    HOST = "host"
    SHARD = "shard"
    CLUSTER = "cluster"


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

    # Create listing table for storing paths from object storage
    try:
        if not use_saved_list:
            _drop_table_on_shard(ctx, listing_table)

        if has_zk():
            create_table_query = f"CREATE TABLE IF NOT EXISTS {listing_table} ON CLUSTER {get_cluster_name(ctx)} (obj_path String, obj_size UInt64) ENGINE ReplicatedMergeTree('{listing_table_zk_path_prefix}/{{shard}}/{listing_table}', '{{replica}}') ORDER BY obj_path SETTINGS storage_policy = '{config['storage_policy']}'"
        else:
            create_table_query = f"CREATE TABLE IF NOT EXISTS {listing_table} (obj_path String, obj_size UInt64) ENGINE MergeTree ORDER BY obj_path SETTINGS storage_policy = '{config['storage_policy']}'"

        execute_query(ctx, create_table_query)
        deleted, total_size = _clean_object_storage(
            ctx,
            object_name_prefix,
            from_time,
            to_time,
            clean_scope,
            cluster_name,
            dry_run,
            listing_table,
            use_saved_list,
        )
    finally:
        if not keep_paths:
            _drop_table_on_shard(ctx, listing_table)

    return deleted, total_size


def _clean_object_storage(
    ctx: Context,
    object_name_prefix: str,
    from_time: Optional[timedelta],
    to_time: timedelta,
    clean_scope: CleanScope,
    cluster_name: str,
    dry_run: bool,
    listing_table: str,
    use_saved_list: bool,
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
        SELECT obj_path, obj_size FROM {listing_table} AS object_storage
          LEFT ANTI JOIN {remote_data_paths_table} AS object_table
          ON object_table.remote_path = object_storage.obj_path
            AND object_table.disk_name = '{disk_conf.name}'
        {settings}
    """,
        sensitive_args={"user_password": user_password},
    )
    logging.info("Antijoin query: {}", str(antijoin_query))

    if dry_run:
        logging.info("Counting orphaned objects...")
    else:
        logging.info("Deleting orphaned objects...")

    deleted = 0
    total_size = 0
    timeout = ctx.obj["config"]["object_storage"]["clean"]["antijoin_timeout"]
    with ch_client.query(
        antijoin_query,
        timeout=timeout,
        settings={"receive_timeout": timeout, "max_execution_time": 0},
        stream=True,
        format_="TabSeparated",
    ) as resp:
        # Setting chunk_size to 10MB, but usually incoming chunks are not larger than 1MB
        for lines in chunked(
            resp.iter_lines(chunk_size=10 * 1024 * 1024), KEYS_BATCH_SIZE
        ):
            keys = (
                ObjListItem.from_tab_separated(line.decode().strip()) for line in lines
            )
            chunk_deleted, chunk_size = cleanup_s3_object_storage(
                disk_conf, keys, dry_run
            )
            deleted += chunk_deleted
            total_size += chunk_size

    logging.info(
        f"{'Would delete' if dry_run else 'Deleted'} {deleted} objects with total size {format_size(total_size, binary=True)} from bucket [{disk_conf.bucket_name}] with prefix {prefix}",
    )

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
