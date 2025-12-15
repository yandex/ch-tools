import os
from typing import Optional

from click import Context

from ch_tools.chadmin.cli.partition_group import get_partitions
from ch_tools.chadmin.internal.partition import attach_partition, detach_partition
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
    get_zk_node,
    list_children,
    zk_client,
)
from ch_tools.common import logging
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration
from ch_tools.common.process_pool import WorkerTask, execute_tasks_in_parallel

ALWAYS_FETCH_ON_ATTACH_SETTING = "always_fetch_instead_of_attach_zero_copy"
DEDUPLICATION_ROOT_PATH = "deduplication"
DEDUPLICATED_TABLES_PATH = os.path.join(DEDUPLICATION_ROOT_PATH, "done")


def deduplicate(
    ctx: Context,
    database: Optional[str],
    table: Optional[str],
    partition_id: Optional[str],
    min_partition_id: Optional[str],
    max_partition_id: Optional[str],
    max_workers: int,
    ignore_saved_state: bool,
    dry_run: bool,
) -> None:
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

        delete_zk_node(
            ctx, _get_table_deduplication_zk_path(table_info), dry_run=dry_run
        )

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
                    create_zk_nodes(
                        ctx,
                        [_get_table_deduplication_zk_path(table_info)],
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
    if check_zk_node(ctx, path):
        return get_zk_node(ctx, path)
    return None


def _get_table_deduplication_zk_path(table: TableInfo) -> str:
    return os.path.join(DEDUPLICATION_ROOT_PATH, table["database"], table["name"])
