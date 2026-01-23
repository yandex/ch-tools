# pylint: disable=too-many-lines
import json
from collections import OrderedDict
from typing import Any, Dict, List, Optional

from click import Context
from cloup import Choice, constraint, group, option, option_group, pass_context
from cloup.constraints import AnySet, If, IsSet, RequireAtLeast, RequireExactly

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.partition import (
    attach_partition,
    detach_partition,
    drop_partition,
    materialize_ttl_in_partition,
    move_partition,
    optimize_partition,
)
from ch_tools.chadmin.internal.table import check_table
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.cli.formatting import print_response
from ch_tools.common.cli.parameters import BytesParamType
from ch_tools.common.process_pool import WorkerTask, execute_tasks_in_parallel


@group("partition", cls=Chadmin)
def partition_group() -> None:
    """
    Commands to manage partitions.
    """
    pass


@partition_group.command("list")
@option(
    "-d",
    "--database",
    help="Filter in partitions to output by the specified database."
    " Multiple values can be specified through a comma.",
)
@option(
    "-t",
    "--table",
    help="Filter in partitions to output by the specified table."
    " Multiple values can be specified through a comma.",
)
@option(
    "--id",
    "--partition",
    "partition_id",
    help="Filter in partitions to output by the specified partition."
    " Multiple values can be specified through a comma.",
)
@option("--min-partition", "min_partition_id")
@option("--max-partition", "max_partition_id")
@option("--min-date")
@option("--max-date")
@option("--min-parts", "--min-part-count", "min_part_count")
@option("--max-parts", "--max-part-count", "max_part_count")
@option(
    "--min-size",
    type=BytesParamType(),
    help="Output partitions which size greater or equal to the specified size.",
)
@option(
    "--max-size",
    type=BytesParamType(),
    help="Output partitions which size less or equal to the specified size.",
)
@option(
    "--disk", "disk_name", help="Filter in partitions to output by the specified disk."
)
@option(
    "--merging",
    is_flag=True,
    help="Output only those partitions that have merging data parts.",
)
@option(
    "--mutating",
    is_flag=True,
    help="Output only those partitions that have mutating data parts.",
)
@option(
    "--has-replication-tasks",
    "has_replication_tasks",
    is_flag=True,
    help="Output only those partitions that are processing by replication queue tasks.",
)
@option(
    "--min-replication-task-postpone-count",
    type=int,
    help="Filter out replication tasks with less than the specified number of postponements.",
)
@option(
    "--max-replication-task-postpone-count",
    type=int,
    help="Filter out replication tasks with more than the specified number of postponements.",
)
@option(
    "--replication-task-error",
    "--replication-task-exception",
    "replication_task_exception",
    help="Filter out replication tasks by the specified exception.",
)
@option(
    "--detached", is_flag=True, help="Show detached partitions instead of attached."
)
@option(
    "--active",
    "--active-parts",
    "active_parts",
    is_flag=True,
    help="Account only active data parts.",
)
@option(
    "--exclude-database",
    "exclude_database_pattern",
    help="Filter out partitions to output by the specified database name pattern."
    " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
)
@option("--order-by", type=Choice(["size", "parts", "rows"]), help="Sorting order.")
@option(
    "-l", "--limit", type=int, help="Limit the max number of objects in the output."
)
@constraint(
    If(
        AnySet(
            "min_replication_task_postpone_count",
            "max_replication_task_postpone_count",
            "replication_task_exception",
        ),
        then=RequireExactly(1),
    ),
    ["has_replication_tasks"],
)
@pass_context
def list_partitions_command(ctx: Context, **kwargs: Any) -> None:
    """List partitions."""
    logging.info(get_partitions(ctx, format_="PrettyCompact", **kwargs))


@partition_group.command("attach")
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
        help="Filter in partitions to attach by the specified database."
        " Multiple values can be specified through a comma.",
    ),
    option(
        "-t",
        "--table",
        help="Filter in partitions to attach by the specified table."
        " Multiple values can be specified through a comma.",
    ),
    option(
        "--id",
        "--partition",
        "partition_id",
        help="Filter in partitions to attach by the specified partition."
        " Multiple values can be specified through a comma.",
    ),
    option("--min-partition", "min_partition_id"),
    option("--max-partition", "max_partition_id"),
    option(
        "--disk",
        "disk_name",
        help="Filter in partitions to attach by the specified disk.",
    ),
    option(
        "--use-partition-list-from-json",
        default=None,
        type=str,
        help="Use list of partitions from the file. Example 'SELECT database, table, partition_id ... FORMAT JSON' > file && chadmin partition attach --use-partition-list-from-json <file>.",
    ),
    constraint=If(
        IsSet("use_partition_list_from_json"),
        then=RequireExactly(1),
        else_=RequireAtLeast(1),
    ),
)
@option("-k", "--keep-going", is_flag=True, help="Do not stop on the first error.")
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def attach_partitions_command(
    ctx: Context,
    _all: bool,
    database: Optional[str],
    table: Optional[str],
    partition_id: Optional[str],
    keep_going: bool,
    dry_run: bool,
    **kwargs: Any,
) -> None:
    """Attach one or several partitions."""
    partitions = get_partitions(
        ctx,
        database,
        table,
        partition_id=partition_id,
        detached=True,
        format_="JSON",
        **kwargs,
    )["data"]
    for p in partitions:
        try:
            attach_partition(
                ctx,
                database=p["database"],
                table=p["table"],
                partition_id=p["partition_id"],
                dry_run=dry_run,
            )
        except Exception as e:
            if keep_going:
                logging.warning("{!r}\n", e)
            else:
                raise


@partition_group.command("detach")
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
        help="Filter in partitions to detach by the specified database."
        " Multiple values can be specified through a comma.",
    ),
    option(
        "-t",
        "--table",
        help="Filter in partitions to detach by the specified table."
        " Multiple values can be specified through a comma.",
    ),
    option(
        "--id",
        "--partition",
        "partition_id",
        help="Filter in partitions to detach by the specified partition."
        " Multiple values can be specified through a comma.",
    ),
    option(
        "--disk",
        "disk_name",
        help="Filter in partitions to detach by the specified disk.",
    ),
    option(
        "--merging",
        is_flag=True,
        help="Filter in only those partitions that have merging data parts.",
    ),
    option(
        "--mutating",
        is_flag=True,
        help="Filter in only those partitions that have mutating data parts.",
    ),
    option(
        "--has-replication-tasks",
        "has_replication_tasks",
        is_flag=True,
        help="Filter in only those partitions that are processing by replication queue tasks.",
    ),
    option(
        "--min-replication-task-postpone-count",
        type=int,
        help="Filter out replication tasks with less than the specified number of postponements.",
    ),
    option(
        "--max-replication-task-postpone-count",
        type=int,
        help="Filter out replication tasks with more than the specified number of postponements.",
    ),
    option(
        "--replication-task-error",
        "--replication-task-exception",
        "replication_task_exception",
        help="Filter out replication tasks by the specified exception.",
    ),
    option(
        "--use-partition-list-from-json",
        default=None,
        type=str,
        help="Use list of partitions from the file. Example 'SELECT database, table, partition_id ... FORMAT JSON' > file && chadmin partition detach --use-partition-list-from-json <file>.",
    ),
    constraint(
        If(
            AnySet(
                "min_replication_task_postpone_count",
                "max_replication_task_postpone_count",
                "replication_task_exception",
            ),
            then=RequireExactly(1),
        ),
        ["has_replication_tasks"],
    ),
    constraint=If(
        IsSet("use_partition_list_from_json"),
        then=RequireExactly(1),
        else_=RequireAtLeast(1),
    ),
)
@option("-k", "--keep-going", is_flag=True, help="Do not stop on the first error.")
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def detach_partitions_command(
    ctx: Context,
    _all: bool,
    database: Optional[str],
    table: Optional[str],
    partition_id: Optional[str],
    disk_name: Optional[str],
    merging: bool,
    mutating: bool,
    has_replication_tasks: bool,
    min_replication_task_postpone_count: Optional[int],
    max_replication_task_postpone_count: Optional[int],
    replication_task_exception: Optional[str],
    keep_going: bool,
    dry_run: bool,
    use_partition_list_from_json: Optional[str],
) -> None:
    """Detach one or several partitions."""
    partitions = get_partitions(
        ctx,
        database,
        table,
        partition_id=partition_id,
        disk_name=disk_name,
        merging=merging,
        mutating=mutating,
        has_replication_tasks=has_replication_tasks,
        min_replication_task_postpone_count=min_replication_task_postpone_count,
        max_replication_task_postpone_count=max_replication_task_postpone_count,
        replication_task_exception=replication_task_exception,
        format_="JSON",
        use_partition_list_from_json=use_partition_list_from_json,
    )["data"]
    for p in partitions:
        try:
            detach_partition(
                ctx,
                database=p["database"],
                table=p["table"],
                partition_id=p["partition_id"],
                dry_run=dry_run,
            )
        except Exception as e:
            if keep_going:
                logging.warning("{!r}\n", e)
            else:
                raise


@partition_group.command("reattach")
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
    option(
        "--disk",
        "disk_name",
        help="Filter in partitions to reattach by the specified disk.",
    ),
    option(
        "--merging",
        is_flag=True,
        help="Filter in only those partitions that have merging data parts.",
    ),
    option(
        "--mutating",
        is_flag=True,
        help="Filter in only those partitions that have mutating data parts.",
    ),
    option(
        "--has-replication-tasks",
        "has_replication_tasks",
        is_flag=True,
        help="Filter in only those partitions that are processing by replication queue tasks.",
    ),
    option(
        "--min-replication-task-postpone-count",
        type=int,
        help="Filter out replication tasks with less than the specified number of postponements.",
    ),
    option(
        "--max-replication-task-postpone-count",
        type=int,
        help="Filter out replication tasks with more than the specified number of postponements.",
    ),
    option(
        "--replication-task-error",
        "--replication-task-exception",
        "replication_task_exception",
        help="Filter out replication tasks by the specified exception.",
    ),
    option(
        "-l",
        "--limit",
        type=int,
        help="Limit the max number of partitions to reattach in the output.",
    ),
    option(
        "--use-partition-list-from-json",
        default=None,
        type=str,
        help="Use list of partitions from the file. Example 'SELECT database, table, partition_id ... FORMAT JSON' > file && chadmin partition rettach --use-partition-list-from-json <file>.",
    ),
    constraint(
        If(
            AnySet(
                "min_replication_task_postpone_count",
                "max_replication_task_postpone_count",
                "replication_task_exception",
            ),
            then=RequireExactly(1),
        ),
        ["has_replication_tasks"],
    ),
    constraint=If(
        IsSet("use_partition_list_from_json"),
        then=RequireExactly(1),
        else_=RequireAtLeast(1),
    ),
)
@option("-k", "--keep-going", is_flag=True, help="Do not stop on the first error.")
@option(
    "--limit-errors",
    type=int,
    help="Limit the max number of failed to detach or attach partitions before exit if keep-going is set.",
    default=10,
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def reattach_partitions_command(
    ctx: Context,
    _all: bool,
    database: Optional[str],
    table: Optional[str],
    partition_id: Optional[str],
    min_partition_id: Optional[str],
    max_partition_id: Optional[str],
    disk_name: Optional[str],
    merging: bool,
    mutating: bool,
    has_replication_tasks: bool,
    min_replication_task_postpone_count: Optional[int],
    max_replication_task_postpone_count: Optional[int],
    replication_task_exception: Optional[str],
    limit: Optional[int],
    keep_going: bool,
    limit_errors: int,
    dry_run: bool,
    use_partition_list_from_json: Optional[str],
) -> None:
    """Perform sequential attach and detach of one or several partitions."""

    def _table_formatter(partition: Dict[str, Any]) -> OrderedDict:
        return OrderedDict(
            (
                ("database", partition["database"]),
                ("table", partition["table"]),
                ("partition_id", partition["partition_id"]),
            )
        )

    partitions = get_partitions(
        ctx,
        database,
        table,
        partition_id=partition_id,
        min_partition_id=min_partition_id,
        max_partition_id=max_partition_id,
        disk_name=disk_name,
        merging=merging,
        mutating=mutating,
        has_replication_tasks=has_replication_tasks,
        min_replication_task_postpone_count=min_replication_task_postpone_count,
        max_replication_task_postpone_count=max_replication_task_postpone_count,
        replication_task_exception=replication_task_exception,
        limit=limit,
        format_="JSON",
        use_partition_list_from_json=use_partition_list_from_json,
    )["data"]

    error_count = 0
    failed_partitions = []
    for p in partitions:
        try:
            detach_partition(
                ctx, p["database"], p["table"], p["partition_id"], dry_run=dry_run
            )
            attach_partition(
                ctx, p["database"], p["table"], p["partition_id"], dry_run=dry_run
            )
        except Exception:
            error_count += 1
            failed_partitions.append(p)
            if not keep_going:
                raise
            if error_count == limit_errors:
                logging.info("Max number of errors reached.")
                break

    if failed_partitions:
        print("Partitions that failed to detach or attach:")
        print_response(
            ctx,
            failed_partitions,
            default_format="table",
            table_formatter=_table_formatter,
        )


@partition_group.command("delete")
@option_group(
    "Partition selection options",
    option(
        "-d",
        "--database",
        help="Filter in partitions to delete by the specified database."
        " Multiple values can be specified through a comma.",
    ),
    option(
        "-t",
        "--table",
        help="Filter in partitions to delete by the specified table."
        " Multiple values can be specified through a comma.",
    ),
    option(
        "--id",
        "--partition",
        "partition_id",
        help="Filter in partitions to delete by the specified partition."
        " Multiple values can be specified through a comma.",
    ),
    option("--min-partition", "min_partition_id"),
    option("--max-partition", "max_partition_id"),
    option("--min-date"),
    option("--max-date"),
    option(
        "--disk",
        "disk_name",
        help="Filter in partitions to delete by the specified disk.",
    ),
    option(
        "--use-partition-list-from-json",
        default=None,
        type=str,
        help="Use list of partitions from the file. Example 'SELECT database, table, partition_id ... FORMAT JSON' > file && chadmin partition attach --use-partition-list-from-json <file>.",
    ),
    constraint=RequireAtLeast(1),
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def delete_partitions_command(
    ctx: Context,
    database: Optional[str],
    table: Optional[str],
    partition_id: Optional[str],
    min_partition_id: Optional[str],
    max_partition_id: Optional[str],
    min_date: Optional[str],
    max_date: Optional[str],
    disk_name: Optional[str],
    dry_run: bool,
    use_partition_list_from_json: Optional[str],
) -> None:
    """Delete one or several partitions."""
    partitions = get_partitions(
        ctx,
        database,
        table,
        partition_id=partition_id,
        min_partition_id=min_partition_id,
        max_partition_id=max_partition_id,
        min_date=min_date,
        max_date=max_date,
        disk_name=disk_name,
        format_="JSON",
        use_partition_list_from_json=use_partition_list_from_json,
    )["data"]
    for p in partitions:
        drop_partition(
            ctx, p["database"], p["table"], p["partition_id"], dry_run=dry_run
        )


@partition_group.command("optimize")
@option_group(
    "Partition selection options",
    option(
        "-d",
        "--database",
        help="Filter in partitions to optimize by the specified database."
        " Multiple values can be specified through a comma.",
    ),
    option(
        "-t",
        "--table",
        help="Filter in partitions to optimize by the specified table."
        " Multiple values can be specified through a comma.",
    ),
    option(
        "--id",
        "--partition",
        "partition_id",
        help="Filter in partitions to optimize by the specified partition."
        " Multiple values can be specified through a comma.",
    ),
    option("--min-partition", "min_partition_id"),
    option("--max-partition", "max_partition_id"),
    option("--min-date"),
    option("--max-date"),
    option(
        "--disk",
        "disk_name",
        help="Filter in partitions to optimize by the specified disk.",
    ),
    constraint=RequireAtLeast(1),
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def optimize_partitions_command(
    ctx: Context,
    database: Optional[str],
    table: Optional[str],
    partition_id: Optional[str],
    min_partition_id: Optional[str],
    max_partition_id: Optional[str],
    min_date: Optional[str],
    max_date: Optional[str],
    disk_name: Optional[str],
    dry_run: bool,
) -> None:
    """Optimize partitions."""
    for p in get_partitions(
        ctx,
        database,
        table,
        partition_id=partition_id,
        min_partition_id=min_partition_id,
        max_partition_id=max_partition_id,
        min_date=min_date,
        max_date=max_date,
        disk_name=disk_name,
        format_="JSON",
    )["data"]:
        optimize_partition(
            ctx, p["database"], p["table"], p["partition_id"], dry_run=dry_run
        )


@partition_group.command("materialize-ttl")
@option_group(
    "Partition selection options",
    option(
        "-d",
        "--database",
        help="Filter in partitions to materialize TTL by the specified database."
        " Multiple values can be specified through a comma.",
    ),
    option(
        "-t",
        "--table",
        help="Filter in partitions to materialize TTL by the specified table."
        " Multiple values can be specified through a comma.",
    ),
    option(
        "--id",
        "--partition",
        "partition_id",
        help="Filter in partitions to materialize TTL by the specified partition."
        " Multiple values can be specified through a comma.",
    ),
    option("--min-partition", "min_partition_id"),
    option("--max-partition", "max_partition_id"),
    option("--min-date"),
    option("--max-date"),
    option(
        "--disk",
        "disk_name",
        help="Filter in partitions to materialize TTL by the specified disk.",
    ),
    constraint=RequireAtLeast(1),
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def materialize_ttl_command(
    ctx: Context,
    database: Optional[str],
    table: Optional[str],
    partition_id: Optional[str],
    min_partition_id: Optional[str],
    max_partition_id: Optional[str],
    min_date: Optional[str],
    max_date: Optional[str],
    disk_name: Optional[str],
    dry_run: bool,
) -> None:
    """Materialize TTL."""
    for p in get_partitions(
        ctx,
        database,
        table,
        partition_id=partition_id,
        min_partition_id=min_partition_id,
        max_partition_id=max_partition_id,
        min_date=min_date,
        max_date=max_date,
        disk_name=disk_name,
        format_="JSON",
    )["data"]:
        materialize_ttl_in_partition(
            ctx, p["database"], p["table"], p["partition_id"], dry_run=dry_run
        )


def read_and_validate_partitions_from_json(json_path: str) -> Dict[str, Any]:

    base_exception_str = "Incorrect json file, there are no {corrupted_section}. Use the JSON format for ch query to get correct format."
    with open(json_path, "r", encoding="utf-8") as json_file:
        json_obj = json.load(json_file)
        if "data" not in json_obj:
            raise ValueError(base_exception_str.format("data section"))

        partitions_list = json_obj["data"]
        for p in partitions_list:
            if "database" not in p:
                raise ValueError(base_exception_str.format("database"))
            if "table" not in p:
                raise ValueError(base_exception_str.format("table"))
            if "partition_id" not in p and "partition" not in p:
                raise ValueError(base_exception_str.format("partition_id/partition"))
    return json_obj


@partition_group.command("check")
@option_group(
    "Partition selection options",
    option(
        "-d",
        "--database",
        help="Filter in partitions to check by the specified database."
        " Multiple values can be specified through a comma.",
    ),
    option(
        "-t",
        "--table",
        help="Filter in partitions to check by the specified table."
        " Multiple values can be specified through a comma.",
    ),
    option(
        "--id",
        "--partition",
        "partition_id",
        help="Filter in partitions to check by the specified partition."
        " Multiple values can be specified through a comma.",
    ),
    option("--min-partition", "min_partition_id"),
    option("--max-partition", "max_partition_id"),
    option("--disk", "disk_name"),
    constraint=RequireAtLeast(1),
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def check_command(
    ctx: Context,
    database: Optional[str],
    table: Optional[str],
    partition_id: Optional[str],
    min_partition_id: Optional[str],
    max_partition_id: Optional[str],
    disk_name: Optional[str],
    dry_run: bool,
) -> None:
    """Check partitions"""
    result: List[Dict[str, Any]] = []
    for p in get_partitions(
        ctx,
        database,
        table,
        partition_id=partition_id,
        min_partition_id=min_partition_id,
        max_partition_id=max_partition_id,
        disk_name=disk_name,
        format_="JSON",
    )["data"]:
        result.append(
            {
                "table": f"{p['database']}.{p['table']}",
                "partition": p["partition_id"],
                "result": check_table(
                    ctx,
                    p["database"],
                    p["table"],
                    dry_run=dry_run,
                    partition=p["partition_id"],
                ),
            }
        )
    print_response(ctx, result, default_format="table")


@partition_group.command("move")
@option("-D", "--dst-database", required=True, help="The destinatiton database.")
@option("-T", "--dst-table", required=True, help="The destinatiton table")
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
        "--src-database",
        help="Filter in partitions to move by the specified database."
        " Multiple values can be specified through a comma.",
    ),
    option(
        "-t",
        "--src-table",
        help="Filter in partitions to move by the specified table."
        " Multiple values can be specified through a comma.",
    ),
    option(
        "--id",
        "--partition",
        "partition_id",
        help="Filter in partitions to move by the specified partition."
        " Multiple values can be specified through a comma.",
    ),
    option("--min-partition", "min_partition_id"),
    option("--max-partition", "max_partition_id"),
    option(
        "--disk",
        "disk_name",
        help="Filter in partitions to move by the specified disk.",
    ),
    option(
        "--use-partition-list-from-json",
        default=None,
        type=str,
        help="Use list of partitions from the file. Example 'SELECT database, table, partition_id ... FORMAT JSON' > file && chadmin partition move --use-partition-list-from-json <file>.",
    ),
    constraint=If(
        IsSet("use_partition_list_from_json"),
        then=RequireExactly(1),
        else_=RequireAtLeast(1),
    ),
)
@option("-k", "--keep-going", is_flag=True, help="Do not stop on the first error.")
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@option("-w", "--workers", default=4, help="Number of workers.")
@pass_context
def move_partitions_command(
    ctx: Context,
    dst_database: str,
    dst_table: str,
    _all: bool,
    src_database: Optional[str],
    src_table: Optional[str],
    partition_id: Optional[str],
    keep_going: bool,
    dry_run: bool,
    workers: int,
    **kwargs: Any,
) -> None:
    """Move one or severral"""
    partitions = get_partitions(
        ctx,
        src_database,
        src_table,
        partition_id=partition_id,
        format_="JSON",
        **kwargs,
    )["data"]
    tasks: List[WorkerTask] = []
    for p in partitions:
        tasks.append(
            WorkerTask(
                f'move_part_{p["database"]}.{p["table"]}_{p["partition"]}',
                move_partition,
                {
                    "ctx": ctx,
                    "src_database": p["database"],
                    "src_table": p["table"],
                    "partition": p["partition"],
                    "dst_database": dst_database,
                    "dst_table": dst_table,
                    "dry_run": dry_run,
                },
            )
        )

    execute_tasks_in_parallel(tasks, max_workers=workers, keep_going=keep_going)


def get_partitions(
    ctx: Context,
    database: Optional[str],
    table: Optional[str],
    *,
    partition_id: Optional[str] = None,
    min_partition_id: Optional[str] = None,
    max_partition_id: Optional[str] = None,
    min_date: Optional[str] = None,
    max_date: Optional[str] = None,
    min_part_count: Optional[int] = None,
    max_part_count: Optional[int] = None,
    min_size: Optional[int] = None,
    max_size: Optional[int] = None,
    active_parts: Optional[bool] = None,
    disk_name: Optional[str] = None,
    merging: Optional[bool] = None,
    mutating: Optional[bool] = None,
    has_replication_tasks: Optional[bool] = None,
    min_replication_task_postpone_count: Optional[int] = None,
    max_replication_task_postpone_count: Optional[int] = None,
    replication_task_exception: Optional[str] = None,
    detached: Optional[bool] = None,
    order_by: Optional[str] = None,
    limit: Optional[int] = None,
    format_: Optional[str] = None,
    use_partition_list_from_json: Optional[str] = None,
    exclude_database_pattern: Optional[str] = None,
) -> Any:
    # pylint: disable=too-many-locals,too-many-arguments
    if use_partition_list_from_json:
        return read_and_validate_partitions_from_json(use_partition_list_from_json)

    order_by = {
        "size": "sum(bytes_on_disk) DESC",
        "parts": "parts DESC",
        "rows": "rows DESC",
        None: "database, table, partition_id",
    }[order_by]

    if detached:
        query = """
            SELECT
                database,
                table,
                partition_id,
                count() "parts"
            FROM system.detached_parts
            {% if database -%}
              WHERE database {{ format_str_match(database) }}
            {% else -%}
              WHERE database NOT IN ('information_schema', 'INFORMATION_SCHEMA')
            {% endif -%}
            {% if exclude_database_pattern -%}
              AND database NOT {{ format_str_match(exclude_database_pattern) }}
            {% endif -%}
            {% if table -%}
              AND table {{ format_str_match(table) }}
            {% endif -%}
            GROUP BY database, table, partition_id
            HAVING partition_id IS NOT NULL
            {% if partition_id -%}
              AND partition_id {{ format_str_match(partition_id) }}
            {% endif -%}
            {% if min_partition_id -%}
              AND partition_id >= '{{ min_partition_id }}'
            {% endif -%}
            {% if max_partition_id -%}
              AND partition_id <= '{{ max_partition_id }}'
            {% endif -%}
            {% if min_part_count -%}
              AND parts >= {{ min_part_count }}
            {% endif -%}
            {% if max_part_count -%}
              AND parts <= {{ max_part_count }}
            {% endif %}
            ORDER BY {{ order_by }}
            {% if limit -%}
            LIMIT {{ limit }}
            {% endif -%}
            """
    else:
        query = """
            SELECT
                database,
                table,
                partition_id,
                partition,
                count() "parts",
                min(min_time) "min_time",
                max(max_time) "max_time",
                arrayStringConcat(groupUniqArray(disk_name), ', ') "disks",
                sum(rows) "rows",
                formatReadableSize(sum(bytes_on_disk)) "bytes"
            FROM system.parts
            {% if database -%}
            WHERE database {{ format_str_match(database) }}
            {% else -%}
            WHERE database NOT IN ('information_schema', 'INFORMATION_SCHEMA')
            {% endif -%}
            {% if exclude_database_pattern -%}
              AND database NOT {{ format_str_match(exclude_database_pattern) }}
            {% endif -%}
            {% if active_parts -%}
              AND active
            {% endif -%}
            {% if table -%}
              AND table {{ format_str_match(table) }}
            {% endif -%}
            GROUP BY database, table, partition_id, partition
            HAVING 1
            {% if disk_name -%}
               AND has(groupUniqArray(disk_name), '{{ disk_name }}')
            {% endif -%}
            {% if partition_id -%}
               AND partition_id {{ format_str_match(partition_id) }}
            {% endif -%}
            {% if min_partition_id -%}
               AND partition_id >= '{{ min_partition_id }}'
            {% endif -%}
            {% if max_partition_id -%}
               AND partition_id <= '{{ max_partition_id }}'
            {% endif -%}
            {% if min_date -%}
               AND max_date >= '{{ min_date }}'
            {% endif -%}
            {% if max_date -%}
               AND min_date <= '{{ max_date }}'
            {% endif -%}
            {% if min_size -%}
               AND sum(bytes_on_disk) >= '{{ min_size }}'
            {% endif -%}
            {% if max_size -%}
               AND sum(bytes_on_disk) <= '{{ max_size }}'
            {% endif -%}
            {% if merging -%}
               AND (database, table, partition_id) IN (
                   SELECT (database, table, partition_id)
                   FROM system.merges
               )
            {% endif -%}
            {% if mutating -%}
               AND (database, table, partition_id) IN (
                   SELECT (database, table, partition_id)
                   FROM system.merges
                   WHERE is_mutation
               )
            {% endif -%}
            {% if has_replication_tasks -%}
               AND (database, table, partition_id) IN (
                   SELECT (database, table, splitByChar('_', new_part_name, 1)[1])
                   FROM system.replication_queue
                   WHERE 1
            {%     if min_replication_task_postpone_count -%}
                     AND num_postponed >= {{ min_replication_task_postpone_count }}
            {%     endif -%}
            {%     if max_replication_task_postpone_count -%}
                     AND num_postponed <= {{ max_replication_task_postpone_count }}
            {%     endif -%}
            {%     if replication_task_exception -%}
                     AND last_exception {{ format_str_match(replication_task_exception) }}
            {%     endif -%}
               )
            {% endif -%}
            ORDER BY {{ order_by }}
            {% if limit -%}
            LIMIT {{ limit }}
            {% endif -%}
            """
    return execute_query(
        ctx,
        query,
        database=database,
        exclude_database_pattern=exclude_database_pattern,
        table=table,
        partition_id=partition_id,
        min_partition_id=min_partition_id,
        max_partition_id=max_partition_id,
        min_date=min_date,
        max_date=max_date,
        min_part_count=min_part_count,
        max_part_count=max_part_count,
        min_size=min_size,
        max_size=max_size,
        active_parts=active_parts,
        disk_name=disk_name,
        merging=merging,
        mutating=mutating,
        has_replication_tasks=has_replication_tasks,
        min_replication_task_postpone_count=min_replication_task_postpone_count,
        max_replication_task_postpone_count=max_replication_task_postpone_count,
        replication_task_exception=replication_task_exception,
        order_by=order_by,
        limit=limit,
        format_=format_,
    )
