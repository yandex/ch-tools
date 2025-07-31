from datetime import timedelta
from typing import Any, Optional

from click import Choice, Context, group, option, pass_context
from cloup import constraint
from cloup.constraints import mutually_exclusive
from humanfriendly import format_size

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.object_storage.orphaned_objects_state import (
    OrphanedObjectsState,
)
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

STATE_LOCAL_PATH = "/tmp/object_storage_cleanup_state.json"

DEFAULT_CLUSTER_NAME = "{cluster}"


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
    ctx.obj["disk_configuration"] = (
        ch_config.storage_configuration.s3_disk_configuration(
            disk_name, ctx.obj["config"]["object_storage"]["bucket_name_prefix"]
        )
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
        "from endpoint in clickhouse S3 disk config. If there is no trailing slash it will be added automatically."
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
    default=DEFAULT_CLUSTER_NAME,
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
@option(
    "--verify-paths-regex",
    "verify_paths_regex",
    default=None,
    help=("Verify paths to delete and in system.remote_data_paths with regex."),
)
@option(
    "--max-size-to-delete-bytes",
    "max_size_to_delete_bytes",
    type=int,
    default=None,
    help=(
        "Maximum total size (in bytes) of objects to delete. "
        "Must be non-negative."
        "Deletion will stop once this limit is reached."
    ),
)
@option(
    "--max-size-to-delete-fraction",
    "max_size_to_delete_fraction",
    type=float,
    default=None,
    help=(
        "Maximum fraction size of objects to delete."
        "Must be in range [0.0; 1.0]"
        "Deletion will stop once this limit is reached."
    ),
)
@constraint(
    mutually_exclusive, ["max_size_to_delete_fraction", "max_size_to_delete_bytes"]
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
    verify_paths_regex: Optional[str],
    max_size_to_delete_bytes: Optional[int],
    max_size_to_delete_fraction: Optional[float],
) -> None:
    """
    Clean orphaned S3 objects.
    """
    deleted = 0
    total_size = 0
    error_msg = ""

    # Need for correctly call format in Query
    if cluster_name == DEFAULT_CLUSTER_NAME:
        cluster_name = "{" + cluster_name + "}"

    try:
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
            verify_paths_regex,
            max_size_to_delete_bytes,
            max_size_to_delete_fraction,
        )
    finally:
        state = OrphanedObjectsState(total_size, error_msg)

        if store_state_zk_path:
            _store_state_zk_save(ctx, store_state_zk_path, state)

        if store_state_local:
            _store_state_local_save(ctx, state)

    _print_response(ctx, dry_run, deleted, total_size)


def _store_state_zk_save(ctx: Context, path: str, state: OrphanedObjectsState) -> None:
    if not check_zk_node(ctx, path):
        create_zk_nodes(ctx, [path], make_parents=True)
    state_data = state.to_json().encode("utf-8")
    update_zk_nodes(ctx, [path], state_data)


def _store_state_local_save(_: Context, state: OrphanedObjectsState) -> None:
    with open(STATE_LOCAL_PATH, "w", encoding="utf-8") as file:
        file.write(state.to_json())


def _print_response(ctx: Context, dry_run: bool, deleted: int, total_size: int) -> None:
    """
    Outputs result of cleaning.
    """
    # List of dicts for print_response()
    clean_stats = [
        {"WouldDelete" if dry_run else "Deleted": deleted, "TotalSize": total_size}
    ]

    def _table_formatter(stats: Any) -> dict[str, Any]:
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
