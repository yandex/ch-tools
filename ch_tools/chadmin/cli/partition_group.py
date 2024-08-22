from collections import OrderedDict

from cloup import Choice, group, option, option_group, pass_context
from cloup.constraints import RequireAtLeast

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.partition import (
    attach_partition,
    detach_partition,
    drop_partition,
    materialize_ttl_in_partition,
    optimize_partition,
)
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.cli.formatting import print_response
from ch_tools.common.cli.parameters import BytesParamType


@group("partition", cls=Chadmin)
def partition_group():
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
@option("--order-by", type=Choice(["size", "parts", "rows"]), help="Sorting order.")
@option(
    "-l", "--limit", type=int, help="Limit the max number of objects in the output."
)
@pass_context
def list_partitions_command(ctx, **kwargs):
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
    constraint=RequireAtLeast(1),
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
    ctx,
    _all,
    database,
    table,
    partition_id,
    keep_going,
    dry_run,
    **kwargs,
):
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
    constraint=RequireAtLeast(1),
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
    ctx,
    _all,
    database,
    table,
    partition_id,
    disk_name,
    merging,
    mutating,
    has_replication_tasks,
    min_replication_task_postpone_count,
    max_replication_task_postpone_count,
    replication_task_exception,
    keep_going,
    dry_run,
):
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
    constraint=RequireAtLeast(1),
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
    ctx,
    _all,
    database,
    table,
    partition_id,
    min_partition_id,
    max_partition_id,
    disk_name,
    merging,
    mutating,
    has_replication_tasks,
    min_replication_task_postpone_count,
    max_replication_task_postpone_count,
    replication_task_exception,
    limit,
    keep_going,
    limit_errors,
    dry_run,
):
    """Perform sequential attach and detach of one or several partitions."""

    def _table_formatter(partition):
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
    ctx,
    database,
    table,
    partition_id,
    min_partition_id,
    max_partition_id,
    min_date,
    max_date,
    disk_name,
    dry_run,
):
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
    ctx,
    database,
    table,
    partition_id,
    min_partition_id,
    max_partition_id,
    min_date,
    max_date,
    disk_name,
    dry_run,
):
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
    ctx,
    database,
    table,
    partition_id,
    min_partition_id,
    max_partition_id,
    min_date,
    max_date,
    disk_name,
    dry_run,
):
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


def get_partitions(
    ctx,
    database,
    table,
    *,
    partition_id=None,
    min_partition_id=None,
    max_partition_id=None,
    min_date=None,
    max_date=None,
    min_part_count=None,
    max_part_count=None,
    min_size=None,
    max_size=None,
    active_parts=None,
    disk_name=None,
    merging=None,
    mutating=None,
    has_replication_tasks=None,
    min_replication_task_postpone_count=None,
    max_replication_task_postpone_count=None,
    replication_task_exception=None,
    detached=None,
    order_by=None,
    limit=None,
    format_=None,
):
    # pylint: disable=too-many-locals
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
              WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
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
            WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
            {% endif -%}
            {% if active_parts -%}
              AND active
            {% endif -%}
            {% if table -%}
              AND table {{ format_str_match(table) }}
            {% endif -%}
            GROUP BY database, table, partition_id
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
