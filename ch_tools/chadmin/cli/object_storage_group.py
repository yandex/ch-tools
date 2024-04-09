import logging
from datetime import datetime, timedelta, timezone
from tempfile import TemporaryFile
from typing import List, Optional

import click
from click import Context, group, option, pass_context
from humanfriendly import format_size

from ch_tools.chadmin.cli import get_clickhouse_config
from ch_tools.chadmin.internal.object_storage import (
    ObjListItem,
    cleanup_s3_object_storage,
    s3_object_storage_iterator,
)
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common.cli.formatting import print_response
from ch_tools.common.cli.parameters import TimeSpanParamType
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration

# The guard interval is used for S3 objects for which metadata is not found.
# And for metadata for which object is not found in S3.
# These objects are not counted if their last modified time fall in the interval from the moment of starting analyzing.
DEFAULT_GUARD_INTERVAL = "24h"
# Batch size for inserts in a listing table
# Set not very big value due to default ClickHouse 'http_max_field_value_size' settings value 128Kb
# TODO: streaming upload in POST body while INSERT
INSERT_BATCH_SIZE = 500
# Use big enough timeout for stream HTTP query
STREAM_TIMEOUT = 10 * 60


@group("object-storage")
@option(
    "-d",
    "--disk",
    "disk_name",
    default="object_storage",
    help="S3 disk name.",
)
@pass_context
def object_storage_group(ctx: Context, disk_name: str) -> None:
    """Commands to manage S3 objects and their metadata."""
    # Restrict excessive boto logging
    _set_boto_log_level(logging.WARNING)

    ch_config = get_clickhouse_config(ctx)
    ctx.obj[
        "disk_configuration"
    ] = ch_config.storage_configuration.s3_disk_configuaration(disk_name)


@object_storage_group.command("clean")
@option(
    "-p",
    "--prefix",
    "--object_name_prefix",
    "object_name_prefix",
    default="",
    help=(
        "Prefix of object name used while listing bucket. By default its value is attempted to parse "
        "from endpoint in clickhouse S3 disk config"
    ),
)
@option(
    "--from-time",
    "from_time",
    default=None,
    type=TimeSpanParamType(),
    help=(
        "Begin of inspecting interval in human-friendly format. "
        "Objects with a modification time falling interval [now - from_time, now - to_time] are considered."
    ),
)
@option(
    "-g",
    "--guard-interval",
    "--to-time",
    "to_time",
    default=DEFAULT_GUARD_INTERVAL,
    type=TimeSpanParamType(),
    help=("End of inspecting interval in human-friendly format."),
)
@option(
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help=("List objects on all hosts in a cluster."),
)
@option(
    "--cluster",
    "cluster_name",
    default="{cluster}",
    help=("Cluster to be cleaned. Default value is macro."),
)
@option(
    "--dry-run",
    "dry_run",
    is_flag=True,
    help=("Do not delete objects. Show only statistics."),
)
@option(
    "--keep-paths",
    "keep_paths",
    is_flag=True,
    help=("Do not delete collected paths of objects from object storage."),
)
@option(
    "--use-saved-list",
    "use_saved_list",
    is_flag=True,
    help=("Use saved object list without traversing object storage again."),
)
@option(
    "--storage-policy",
    "storage_policy",
    default="local",
    help=("Internal table storage policy.."),
)
@pass_context
def clean_command(
    ctx: Context,
    object_name_prefix: str,
    from_time: Optional[timedelta],
    to_time: timedelta,
    on_cluster: bool,
    cluster_name: str,
    dry_run: bool,
    keep_paths: bool,
    use_saved_list: bool,
    storage_policy: str,
) -> None:
    """
    Clean orphaned S3 objects.
    """
    if from_time is not None and to_time <= from_time:
        raise click.BadParameter(
            "'to_time' parameter must be greater than 'from_time'",
            param_hint="--from-time",
        )

    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]
    config = ctx.obj["config"]["object_storage"]["clean"]

    listing_table = f"{config['listing_table_database']}.{config['listing_table_prefix']}{disk_conf.name}"
    # Create listing table for storing paths from object storage
    try:
        execute_query(
            ctx,
            f"CREATE TABLE IF NOT EXISTS {listing_table} (obj_path String, obj_size UInt64) ENGINE MergeTree ORDER BY obj_path SETTINGS storage_policy = '{storage_policy}'",
        )
        _clean_object_storage(
            ctx,
            object_name_prefix,
            from_time,
            to_time,
            on_cluster,
            cluster_name,
            dry_run,
            listing_table,
            use_saved_list,
        )
    finally:
        if not keep_paths:
            execute_query(
                ctx, f"DROP TABLE IF EXISTS {listing_table} SYNC", format_=None
            )


def _clean_object_storage(
    ctx: Context,
    object_name_prefix: str,
    from_time: Optional[timedelta],
    to_time: timedelta,
    on_cluster: bool,
    cluster_name: str,
    dry_run: bool,
    listing_table: str,
    use_saved_list: bool,
) -> None:
    """
    Delete orphaned objects from object storage.
    """
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]
    prefix = object_name_prefix or disk_conf.prefix

    if not use_saved_list:
        logging.info(
            "Collecting objects... (Disk: '%s', Endpoint '%s', Bucket: '%s', Prefix: '%s')",
            disk_conf.name,
            disk_conf.endpoint_url,
            disk_conf.bucket_name,
            prefix,
        )
        _traverse_object_storage(ctx, listing_table, from_time, to_time, prefix)

    remote_data_paths_table = "system.remote_data_paths"
    if on_cluster:
        remote_data_paths_table = (
            f"clusterAllReplicas('{cluster_name}', {remote_data_paths_table})"
        )

    antijoin_query = f"""
        SELECT obj_path, obj_size FROM {listing_table} AS object_storage
          LEFT ANTI JOIN {remote_data_paths_table} AS object_table
          ON object_table.remote_path = object_storage.obj_path
            AND object_table.disk_name = '{disk_conf.name}'
    """
    logging.info("Antijoin query: %s", antijoin_query)

    if dry_run:
        logging.info("Counting orphaned objects...")
    else:
        logging.info("Deleting orphaned objects...")

    deleted = 0
    total_size = 0
    with TemporaryFile() as keys_file:
        with execute_query(
            ctx, antijoin_query, stream=True, format_="TabSeparated"
        ) as resp:
            # Save response to the file by chunks
            for chunk in resp.iter_content(chunk_size=8192):
                keys_file.write(chunk)

        keys_file.seek(0)  # rewind file pointer to the beginning

        keys = (
            ObjListItem.from_tab_separated(line.decode().strip()) for line in keys_file
        )
        deleted, total_size = cleanup_s3_object_storage(disk_conf, keys, dry_run)

    logging.info(
        "%s %s objects with total size %s from bucket [%s] with prefix %s",
        "Would delete" if dry_run else "Deleted",
        deleted,
        format_size(total_size, binary=True),
        disk_conf.bucket_name,
        prefix,
    )
    _print_response(ctx, dry_run, deleted, total_size)


def _print_response(ctx: Context, dry_run: bool, deleted: int, total_size: int) -> None:
    """
    Outputs result of cleaning.
    """
    # List of dicts for print_response()
    clean_stats = [
        {"WouldDelete" if dry_run else "Deleted": deleted, "TotalSize": total_size}
    ]

    def _table_formatter(stats):
        result = {}

        if "Deleted" in stats:
            result["Deleted"] = stats["Deleted"]
        if "WouldDeleted" in stats:
            result["WouldDeleted"] = stats["WouldDeleted"]
        result["TotalSize"] = format_size(stats["TotalSize"], binary=True)

        return result

    print_response(
        ctx, clean_stats, default_format="table", table_formatter=_table_formatter
    )


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

    logging.info("Collected %s objects", counter)


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


def _set_boto_log_level(level: int) -> None:
    """
    Set log level for libraries involved in communications with S3.
    """
    logging.getLogger("boto3").setLevel(level)
    logging.getLogger("botocore").setLevel(level)
    logging.getLogger("nose").setLevel(level)
    logging.getLogger("s3transfer").setLevel(level)
    logging.getLogger("urllib3").setLevel(level)
