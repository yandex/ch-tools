import json
from datetime import timedelta
from typing import Optional

from click import Choice, Context, group, option, pass_context
from humanfriendly import format_size

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.zookeeper import (
    check_zk_node,
    create_zk_nodes,
    update_zk_nodes,
)
from ch_tools.common.cli.formatting import print_response
from ch_tools.common.cli.parameters import TimeSpanParamType
from ch_tools.common.clickhouse.config import get_clickhouse_config
from ch_tools.common.commands.clean_object_storage import (
    DEFAULT_GUARD_INTERVAL,
    CleanScope,
    clean,
)

# Use big enough timeout for stream HTTP query
STREAM_TIMEOUT = 10 * 60

ORPHANED_OBJECTS_SIZE_FIELD = "orphaned_objects_size"
STATE_LOCAL_PATH = "/tmp/object_storage_cleanup_state.json"


@group("object-storage", cls=Chadmin)
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
    ] = ch_config.storage_configuration.s3_disk_configuration(
        disk_name, ctx.obj["config"]["object_storage"]["bucket_name_prefix"]
    )


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
    "--clean-scope",
    "clean_scope",
    default="shard",
    type=Choice(["host", "shard", "cluster"]),
    help="Cleaning scope.",
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
    "--store-state-local",
    "store_state_local",
    is_flag=True,
    help=("Write total size of orphaned objects to log file."),
)
@option(
    "--store-state-zk-path",
    "store_state_zk_path",
    default="",
    help=("Zookeeper node path for storage total size of orphaned objects."),
)
@pass_context
def clean_command(
    ctx: Context,
    object_name_prefix: str,
    from_time: Optional[timedelta],
    to_time: timedelta,
    clean_scope: str,
    cluster_name: str,
    dry_run: bool,
    keep_paths: bool,
    use_saved_list: bool,
    store_state_local: bool,
    store_state_zk_path: str,
) -> None:
    """
    Clean orphaned S3 objects.
    """
    deleted, total_size = clean(
        ctx,
        object_name_prefix,
        from_time,
        to_time,
        CleanScope(clean_scope),
        cluster_name,
        dry_run,
        keep_paths,
        use_saved_list,
    )

    if store_state_zk_path:
        _store_state_zk_save(ctx, store_state_zk_path, total_size)

    if store_state_local:
        _store_state_local_save(ctx, total_size)

    _print_response(ctx, dry_run, deleted, total_size)


def _store_state_zk_save(ctx: Context, path: str, total_size: int) -> None:
    if not check_zk_node(ctx, path):
        create_zk_nodes(ctx, [path], make_parents=True)
    state_data = json.dumps({ORPHANED_OBJECTS_SIZE_FIELD: total_size}, indent=4)
    update_zk_nodes(ctx, [path], state_data.encode("utf-8"))


def _store_state_local_save(_: Context, total_size: int) -> None:
    with open(STATE_LOCAL_PATH, "w", encoding="utf-8") as file:
        json.dump({ORPHANED_OBJECTS_SIZE_FIELD: total_size}, file, indent=4)


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
