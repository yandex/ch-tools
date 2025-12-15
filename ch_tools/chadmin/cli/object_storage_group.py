import os
from datetime import timedelta
from typing import Any, Optional

from click import (
    BadParameter,
    Choice,
    Context,
    FloatRange,
    IntRange,
    group,
    option,
    pass_context,
)
from cloup import option_group
from humanfriendly import format_size

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.cli.partition_group import get_partitions
from ch_tools.chadmin.internal.object_storage.orphaned_objects_state import (
    OrphanedObjectsState,
)
from ch_tools.chadmin.internal.object_storage.s3_cleanup_stats import (
    ResultStat,
    StatisticsPeriod,
)
from ch_tools.chadmin.internal.partition import attach_partition, detach_partition
from ch_tools.chadmin.internal.system import match_ch_version
from ch_tools.chadmin.internal.table import (
    has_data_on_disk,
    list_tables,
    set_table_setting,
)
from ch_tools.chadmin.internal.table_info import TableInfo
from ch_tools.chadmin.internal.utils import execute_query_on_shard
from ch_tools.chadmin.internal.zookeeper import (
    check_zk_node,
    create_zk_nodes,
    delete_zk_node,
    list_children,
    update_zk_nodes,
    zk_client,
)
from ch_tools.common import logging
from ch_tools.common.cli.formatting import format_bytes, print_response
from ch_tools.common.cli.parameters import TimeSpanParamType
from ch_tools.common.clickhouse.config import get_clickhouse_config
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration
from ch_tools.common.commands.object_storage import (
    DEFAULT_GUARD_INTERVAL,
    Scope,
    clean,
    collect_object_storage_info,
    get_object_storage_space_usage,
)
from ch_tools.common.process_pool import WorkerTask, execute_tasks_in_parallel

# Use big enough timeout for stream HTTP query
STREAM_TIMEOUT = 10 * 60
STATE_LOCAL_PATH = "/tmp/object_storage_cleanup_state.json"
DEFAULT_CLUSTER_NAME = "{cluster}"
ALWAYS_FETCH_ON_ATTACH_SETTING = (
    "max_suspicious_broken_parts"  # "always_fetch_instead_of_attach_zero_copy"
)
DEDUPLICATION_ROOT_PATH = "deduplication"
DEDUPLICATED_TABLES_PATH = os.path.join(DEDUPLICATION_ROOT_PATH, "done")


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
    ctx.obj["disk_configuration"] = S3DiskConfiguration.from_config(
        ch_config.storage_configuration,
        disk_name,
        ctx.obj["config"]["object_storage"]["bucket_name_prefix"],
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
    type=IntRange(min=0),
    default=0,
    help=(
        "Maximum total size (in bytes) of objects to delete. "
        "Must be non-negative. If 0 then there are no limit."
        "Deletion will stop once this limit is reached."
    ),
)
@option(
    "--max-size-to-delete-fraction",
    "max_size_to_delete_fraction",
    type=FloatRange(min=0.0, max=1.0),
    default=1.0,
    help=(
        "Maximum fraction size of objects to delete."
        "Must be in range [0.0; 1.0]"
        "Deletion will stop once this limit is reached."
    ),
)
@option(
    "--ignore-missing-cloud-storage-backups",
    "ignore_missing_cloud_storage_backups",
    is_flag=True,
    help=(
        "Do not download cloud storage backups if they are not present in shadow directory."
        "This may cause backup corruption."
    ),
)
@option(
    "--stat-by-period",
    "stat_by_period",
    type=Choice(["all", "month", "day"]),
    default=StatisticsPeriod.ALL,
    help=("Partition output stats by months or days, or return only total stats."),
)
@pass_context
def clean_command(
    ctx: Context,
    object_name_prefix: str,
    from_time: Optional[timedelta],
    to_time: timedelta,
    cluster_name: str,
    dry_run: bool,
    keep_paths: bool,
    use_saved_list: bool,
    store_state_local: bool,
    store_state_zk_path: str,
    verify_paths_regex: Optional[str],
    max_size_to_delete_bytes: int,
    max_size_to_delete_fraction: float,
    ignore_missing_cloud_storage_backups: bool,
    stat_by_period: StatisticsPeriod,
) -> None:
    """
    Clean orphaned S3 objects.
    """
    result_stat = ResultStat(stat_by_period)
    error_msg = ""

    # Need for correctly call format in Query
    if cluster_name == DEFAULT_CLUSTER_NAME:
        cluster_name = "{" + cluster_name + "}"

    try:
        result_stat = clean(
            ctx,
            object_name_prefix,
            from_time,
            to_time,
            dry_run,
            keep_paths,
            use_saved_list,
            verify_paths_regex,
            max_size_to_delete_bytes,
            max_size_to_delete_fraction,
            ignore_missing_cloud_storage_backups,
            stat_by_period,
        )
    finally:
        total_size = result_stat.total["total_size"]
        state = OrphanedObjectsState(total_size, error_msg)

        if store_state_zk_path:
            _store_state_zk_save(
                ctx, store_state_zk_path, state.to_json().encode("utf-8")
            )

        if store_state_local:
            _store_state_local_save(ctx, state)

    _print_response(ctx, dry_run, result_stat)


@object_storage_group.command("collect-info")
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
    "--use-saved-list/--traverse-remote",
    "use_saved_list",
    default=True,
    help=("Use saved remote blobs list without traversing object storage again."),
)
@pass_context
def collect_info_command(
    ctx: Context,
    object_name_prefix: str,
    from_time: Optional[timedelta],
    to_time: timedelta,
    use_saved_list: bool,
) -> None:
    """
    Collect info about object storage memory usage and orphaned objects.
    Save the result to the corresponding tables.
    """
    if from_time is not None and to_time < from_time:
        raise BadParameter(
            "'from_time' parameter must be greater than 'to_time'",
            param_hint="--from-time",
        )

    if not match_ch_version(ctx, "23.3"):
        raise RuntimeError("This command requires version 23.3 of ClickHouse")

    collect_object_storage_info(
        ctx,
        object_name_prefix=object_name_prefix,
        from_time=from_time,
        to_time=to_time,
        use_saved=use_saved_list,
    )


@object_storage_group.command("space-usage")
@option(
    "--cluster",
    "cluster_name",
    default=DEFAULT_CLUSTER_NAME,
    help=("Cluster to analyze. Default value is macro."),
)
@option(
    "--scope",
    "scope",
    default="cluster",
    type=Choice(["shard", "cluster"]),
    help="Get info for shard or cluster.",
)
@option(
    "--human-readable",
    "human_readable",
    is_flag=True,
    default=False,
    help="Output size in human readable format",
)
@pass_context
def space_usage_command(
    ctx: Context, cluster_name: str, scope: str, human_readable: bool
) -> None:
    """
    Return object storage memory usage info from cluster.
    """
    result = get_object_storage_space_usage(ctx, cluster_name, Scope(scope))

    field_formatters = {
        "active": format_bytes,
        "unique_frozen": format_bytes,
        "unique_detached": format_bytes,
        "orphaned": format_bytes,
    }

    print_response(
        ctx, result, field_formatters=field_formatters if human_readable else None
    )


@object_storage_group.command("deduplicate")
@option_group(
    "Partition selection options",
    option(
        "-a",
        "--all",
        "_all",
        is_flag=True,
        help="Filter in all partitions.",
    ),
    option(
        "-d",
        "--database",
        help="Filter in partitions to reattach by the specified database."
        " Multiple values can be specified through a comma.",
    ),
    option(
        "-t",
        "--table",
        help="Filter in partitions to reattach by the specified table."
        " Multiple values can be specified through a comma.",
    ),
    option(
        "--id",
        "--partition",
        "partition_id",
        help="Filter in partitions to reattach by the specified partition."
        " Multiple values can be specified through a comma.",
    ),
    option("--min-partition", "min_partition_id"),
    option("--max-partition", "max_partition_id"),
)
@option(
    "--max-workers",
    "max_workers",
    default=4,
    help="Max concurrent tables to deduplicate.",
)
@option(
    "--ignore-saved-state",
    "ignore_saved_state",
    is_flag=True,
    help=(
        "Do not load already processed tables to skip them. Also do not load partition ids from the previous unsuccessful attempt to use it as min_partition_id."
    ),
)
@option(
    "--dry-run",
    "dry_run",
    is_flag=True,
    help=("Do not delete objects. Show only statistics."),
)
@pass_context
def deduplicate_command(
    ctx: Context,
    _all: bool,
    database: Optional[str],
    table: Optional[str],
    partition_id: Optional[str],
    min_partition_id: Optional[str],
    max_partition_id: Optional[str],
    max_workers: int,
    ignore_saved_state: bool,
    dry_run: bool,
) -> None:
    """
    Reattach all partitions with 'always_fetch_instead_of_attach_zero_copy' setting to deduplicate zero-copy parts.
    Requires custom ClickHouse build with copying of part's data on attach instead of detach.
    In case of errors saves state in ZK to use it on retry.
    """
    disk: S3DiskConfiguration = ctx.obj["disk_configuration"]
    deduplicated_tables = (
        set()
        if ignore_saved_state
        else set(list_children(ctx, DEDUPLICATED_TABLES_PATH))
    )

    tables = [
        table
        for table in list_tables(
            ctx,
            database_name=database,
            table_name=table,
            engine_pattern="Replicated%MergeTree",
        )
        if has_data_on_disk(ctx, table, disk.name)
        and f"{table['database']}.{table['name']}" not in deduplicated_tables
    ]

    if not dry_run:
        create_zk_nodes(
            ctx, [DEDUPLICATED_TABLES_PATH], make_parents=True, exists_ok=True
        )

    if deduplicated_tables:
        logging.info("Already deduplicated tables {}", deduplicated_tables)
    logging.info(
        "Will deduplicate zero-copy data for tables {}",
        [f"{table['database']}.{table['name']}" for table in tables],
    )

    tasks = _get_deduplication_tasks(
        ctx,
        disk,
        tables,
        partition_id,
        min_partition_id,
        max_partition_id,
        ignore_saved_state,
        dry_run,
    )

    # Load zk client to context to share it in threads
    with zk_client(ctx):
        execute_tasks_in_parallel(tasks, max_workers, keep_going=False)

    delete_zk_node(ctx, DEDUPLICATION_ROOT_PATH, dry_run=dry_run)


def _get_deduplication_tasks(
    ctx: Context,
    disk: S3DiskConfiguration,
    tables: list[TableInfo],
    partition_id: Optional[str],
    min_partition_id: Optional[str],
    max_partition_id: Optional[str],
    ignore_saved_state: bool,
    dry_run: bool,
) -> list[WorkerTask]:
    def _task(table_info: TableInfo) -> None:
        set_table_setting(
            ctx, table_info, ALWAYS_FETCH_ON_ATTACH_SETTING, 1, dry_run=dry_run
        )

        min_partition_id_actual = (
            min_partition_id
            if ignore_saved_state
            else _get_min_partition_to_deduplicate(ctx, table_info, min_partition_id)
        )

        partitions = get_partitions(
            ctx,
            table_info["database"],
            table_info["name"],
            partition_id=partition_id,
            min_partition_id=min_partition_id_actual,
            max_partition_id=max_partition_id,
            disk_name=disk.name,
            format_="JSON",
        )["data"]

        for p in partitions:
            try:
                detach_partition(
                    ctx, p["database"], p["table"], p["partition_id"], dry_run=dry_run
                )
                attach_partition(
                    ctx, p["database"], p["table"], p["partition_id"], dry_run=dry_run
                )
            except Exception:
                if not dry_run:
                    _store_state_zk_save(
                        ctx,
                        _get_table_deduplication_zk_path(table_info),
                        p["name"],
                    )

        execute_query_on_shard(
            ctx,
            f"SYSTEM SYNC REPLICA {table_info['database']}.{table_info['name']}",
            format_=None,
            dry_run=dry_run,
        )
        set_table_setting(
            ctx, table_info, ALWAYS_FETCH_ON_ATTACH_SETTING, dry_run=dry_run
        )

        if not dry_run:
            create_zk_nodes(
                ctx,
                [
                    os.path.join(
                        DEDUPLICATED_TABLES_PATH,
                        f"{table_info['database']}.{table_info['name']}",
                    )
                ],
            )

    return [
        WorkerTask(f"{table['database']}.{table['name']}", _task, {"table_info": table})
        for table in tables
    ]


def _get_min_partition_to_deduplicate(
    ctx: Context, table: TableInfo, min_partition_id: Optional[str]
) -> Optional[str]:
    min_partition_from_zk = _get_last_reattached_partition_from_zk(ctx, table)
    if min_partition_id or min_partition_from_zk:
        if min_partition_id and min_partition_from_zk:
            return min(min_partition_id, min_partition_from_zk)
        return min_partition_from_zk if min_partition_from_zk else min_partition_id
    return None


def _get_last_reattached_partition_from_zk(
    ctx: Context, table: TableInfo
) -> Optional[str]:
    path = _get_table_deduplication_zk_path(table)
    partitions = list_children(ctx, path)
    return min(partitions) if partitions else None


def _get_table_deduplication_zk_path(table: TableInfo) -> str:
    return os.path.join(DEDUPLICATION_ROOT_PATH, table["database"], table["name"])


def _store_state_zk_save(ctx: Context, path: str, state: Any) -> None:
    if not check_zk_node(ctx, path):
        create_zk_nodes(ctx, [path], make_parents=True)
    state_data = state.to_json().encode("utf-8")
    update_zk_nodes(ctx, [path], state_data)


def _store_state_local_save(_: Context, state: OrphanedObjectsState) -> None:
    with open(STATE_LOCAL_PATH, "w", encoding="utf-8") as file:
        file.write(state.to_json())


def _print_response(ctx: Context, dry_run: bool, result_stat: ResultStat) -> None:
    """
    Outputs result of cleaning.
    """
    # List of dicts for print_response()
    clean_stats = {
        partition: {
            "WouldDelete" if dry_run else "Deleted": values["deleted"],
            "TotalSize": values["total_size"],
        }
        for partition, values in result_stat.items()
    }

    def _table_formatter(stats: Any) -> dict[str, Any]:
        result = {}

        if "Deleted" in stats:
            result["Deleted"] = stats["Deleted"]
        if "WouldDelete" in stats:
            result["WouldDelete"] = stats["WouldDelete"]
        result["TotalSize"] = format_size(stats["TotalSize"], binary=True)

        return result

    print_response(
        ctx, clean_stats, default_format="table", table_formatter=_table_formatter
    )
