from datetime import timedelta
from typing import Optional

from click import Context, group, option, pass_context
from humanfriendly import format_size

from ch_tools.common.cli.formatting import print_response
from ch_tools.common.cli.parameters import TimeSpanParamType
from ch_tools.common.clickhouse.config import get_clickhouse_config
from ch_tools.common.commands.clean_object_storage import DEFAULT_GUARD_INTERVAL, clean

# Use big enough timeout for stream HTTP query
STREAM_TIMEOUT = 10 * 60

ORPHANED_OBJECTS_LOG = "/var/log/s3_orphaned_objects.log"


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
    help=(
        "Do not delete the collected paths from the local table, when the command completes."
    ),
)
@option(
    "--use-saved-list",
    "use_saved_list",
    is_flag=True,
    help=("Use saved object list without traversing object storage again."),
)
@option(
    "--store-state",
    "store_state",
    is_flag=True,
    help=("Write total size of orphaned objects to log file."),
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
    store_state: bool,
) -> None:
    """
    Clean orphaned S3 objects.
    """
    deleted, total_size = clean(
        ctx,
        object_name_prefix,
        from_time,
        to_time,
        on_cluster,
        cluster_name,
        dry_run,
        keep_paths,
        use_saved_list,
    )

    if store_state:
        with open(ORPHANED_OBJECTS_LOG, mode="w+", encoding="utf-8") as file:
            file.write(str(total_size))

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
