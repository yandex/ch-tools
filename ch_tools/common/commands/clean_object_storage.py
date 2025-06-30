import os
import re
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
from ch_tools.chadmin.internal.system import match_ch_version
from ch_tools.chadmin.internal.utils import (
    chunked,
    execute_query,
    execute_query_on_shard,
)
from ch_tools.chadmin.internal.zookeeper import has_zk
from ch_tools.common import logging
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
KEYS_BATCH_SIZE = 100_000


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

    if has_zk():
        create_listing_table_query = f"CREATE TABLE {'IF NOT EXISTS' if recreate_table else ''} {table_name} ON CLUSTER {get_cluster_name(ctx)} (obj_path String, obj_size UInt64) ENGINE ReplicatedMergeTree('{replica_zk_prefix}/{{shard}}/{table_name}', '{{replica}}') ORDER BY obj_path SETTINGS storage_policy = '{storage_policy}'"
    else:
        create_listing_table_query = f"CREATE TABLE {'IF NOT EXISTS' if recreate_table else ''} {table_name} (obj_path String, obj_size UInt64) ENGINE MergeTree ORDER BY obj_path SETTINGS storage_policy = '{storage_policy}'"

    execute_query(ctx, create_listing_table_query)


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


        _create_table_on_shard(
            ctx,
            listing_table,
            config["listing_table_zk_path_prefix"],
            config["storage_policy"],
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
    chunk_size: int = 10 * 1024 * 1024,
) -> Callable:

    def obj_list_iterator() -> Iterator[ObjListItem]:
        with ch_client.query(
            f"SELECT obj_path, obj_size FROM {table_name}",
            format_="TabSeparated",
            settings=query_settings,
            stream=True,
        ) as resp:
            for line in resp.iter_lines(chunk_size=chunk_size):
                yield ObjListItem.from_tab_separated(line.decode().strip())

    return obj_list_iterator


def _sanity_check_before_cleanup(
    ctx: Context,
    orphaned_objects_iterator: Callable,
    ch_client: ClickhouseClient,
    listing_table: str,
    remote_data_paths_table: str,
    verify_paths_regex: Optional[str],
    clean_scope: CleanScope,
) -> None:

    size_error_rate_threshold = ctx.obj["config"]["object_storage"]["clean"][
        "verify_size_error_rate_threshold_bytes"
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

    def perform_check_paths():
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
            raise RuntimeError(
                "Sanity check not passed, because remote_path({}) doesn't matches the regex({}).".format(
                    path_form_ch, paths_regex
                )
            )

        for orphaned_object in orphaned_objects_iterator():
            if not re.match(paths_regex, orphaned_object.path):
                raise RuntimeError(
                    "Sanity check not passed, because orphaned object({}) doesn't matches the regex({}).".format(
                        orphaned_object.path, paths_regex
                    )
                )

    def perform_check_size():
        ### Total size of objects after cleanup must be very close to sum(bytes) FROM system.remote_data_paths
        real_size_in_bucket = int(
            ch_client.query_json_data_first_row(
                query=f"SELECT sum(obj_size) FROM {listing_table}",
                compact=True,
            )[0]
        )

        ch_size_in_bucket = int(
            ch_client.query_json_data_first_row(
                query=Query(
                    f"SELECT sum(size) FROM (SELECT DISTINCT remote_path, size FROM {remote_data_paths_table})",
                    sensitive_args={"user_password": ch_client.password or ""},
                ),
                settings=remote_data_paths_query_settings,
                compact=True,
            )[0]
        )

        size_to_delete = 0
        for orphaned_object in orphaned_objects_iterator():
            size_to_delete += orphaned_object.size

        if (
            abs(real_size_in_bucket - size_to_delete - ch_size_in_bucket)
            > size_error_rate_threshold
        ):
            raise RuntimeError(
                "Sanity check not passed, because after delete size in the bucket will be less than total size of objects known by the clickhouse. Size in CH {} , Total size in bucket {}, would delete {}".format(
                    format_size(ch_size_in_bucket),
                    format_size(real_size_in_bucket),
                    format_size(size_to_delete),
                )
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

    if dry_run:
        logging.info("Counting orphaned objects...")
    else:
        logging.info("Deleting orphaned objects...")

    deleted = 0
    total_size = 0
    timeout = ctx.obj["config"]["object_storage"]["clean"]["antijoin_timeout"]
    query_settings = {"receive_timeout": timeout, "max_execution_time": 0}
    execute_query(
        ctx,
        antijoin_query,
        settings=query_settings,
    )
    orphaned_objects_iterator = _object_list_generator(
        ch_client,
        orphaned_objects_table,
        query_settings,
    )
    if ctx.obj["config"]["object_storage"]["clean"]["verify"]:
        _sanity_check_before_cleanup(
            ctx,
            orphaned_objects_iterator,
            ch_client,
            listing_table,
            remote_data_paths_table,
            verify_paths_regex,
            clean_scope,
        )

    deleted, total_size = 0, 0
    for objects in chunked(orphaned_objects_iterator(), KEYS_BATCH_SIZE):
        chunk_deleted, chunk_size = cleanup_s3_object_storage(
            disk_conf, iter(objects), dry_run
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
