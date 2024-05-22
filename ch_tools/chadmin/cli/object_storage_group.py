from datetime import timedelta
from tempfile import TemporaryFile
from typing import List, Optional

import click
from click import Context, group, option, pass_context
from humanfriendly import format_size

from ch_tools.chadmin.internal.object_storage import (
    ObjListItem,
    cleanup_s3_object_storage,
)
from ch_tools.chadmin.internal.object_storage.utils import get_orphaned_objects_query, get_remote_data_paths_table, get_traverse_shadow_settings, traverse_object_storage, DEFAULT_GUARD_INTERVAL
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.cli.formatting import print_response
from ch_tools.common.cli.parameters import TimeSpanParamType
from ch_tools.common.clickhouse.config import get_clickhouse_config
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration

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
            f"CREATE TABLE IF NOT EXISTS {listing_table} (obj_path String, obj_size UInt64) ENGINE MergeTree ORDER BY obj_path SETTINGS storage_policy = '{config['storage_policy']}'",
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
            f"Collecting objects... (Disk: '{disk_conf.name}', Endpoint '{disk_conf.endpoint_url}', Bucket: '{disk_conf.bucket_name}', Prefix: '{disk_conf.prefix}')",
        )
        counter = traverse_object_storage(ctx, listing_table, from_time, to_time, prefix)
        logging.info("Collected {} objects", counter)

    remote_data_paths_table = get_remote_data_paths_table(on_cluster, cluster_name)
    settings = get_traverse_shadow_settings(ctx)
    antijoin_query = get_orphaned_objects_query(listing_table, remote_data_paths_table, disk_conf, settings)
    logging.info("Antijoin query: {}", antijoin_query)

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
        f"{'Would delete' if dry_run else 'Deleted'} {deleted} objects with total size {format_size(total_size, binary=True)} from bucket [{disk_conf.bucket_name}] with prefix {prefix}",
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
