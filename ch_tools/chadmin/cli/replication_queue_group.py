from collections import defaultdict
from typing import Any, Optional, Union

from click import Context, group, option, pass_context

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.chadmin.internal.zookeeper import delete_zk_node
from ch_tools.common import logging
from ch_tools.common.cli.parameters import TimeSpanParamType
from ch_tools.common.clickhouse.client import OutputFormat
from ch_tools.common.clickhouse.config import get_cluster_name


@group("replication-queue", cls=Chadmin)
def replication_queue_group() -> None:
    """
    Commands to manage replication queue.
    """
    pass


@replication_queue_group.command("list")
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Get records from all hosts in the cluster.",
)
@option(
    "--failed",
    is_flag=True,
    help="Output only failed replication queue tasks (tasks with non-empty exception).",
)
@option(
    "--error",
    "--exception",
    "exception",
    help="Filter replication queue tasks to output by the specified exception.",
)
@option(
    "--executing",
    is_flag=True,
    help="Output only executing replication queue tasks.",
)
@option(
    "--age",
    "--min-age",
    "min_age",
    type=TimeSpanParamType(),
    help="Output only replication queue tasks that were created at least the specified amount of time ago.",
)
@option(
    "--type",
    "type_",
    help="Filter replication queue tasks to output by the specified type."
    " Multiple values can be specified through a comma.",
)
@option(
    "--exclude-type",
    help="Filter replication queue tasks to not output by the specified type."
    " Multiple values can be specified through a comma.",
)
@option(
    "-d",
    "--database",
    help="Filter replication queue tasks to output by the specified database."
    " Multiple values can be specified through a comma.",
)
@option(
    "-t",
    "--table",
    help="Filter replication queue tasks to output by the specified table."
    " Multiple values can be specified through a comma.",
)
@option(
    "--id",
    "--partition",
    "partition_id",
    help="Filter in replication queue tasks to output by the specified partition."
    " Multiple values can be specified through a comma.",
)
@option(
    "--min-postpone-count",
    type=int,
    help="Filter out replication queue tasks with less than the specified number of postponements.",
)
@option(
    "--max-postpone-count",
    type=int,
    help="Filter out replication queue tasks with more than the specified number of postponements.",
)
@option("-v", "--verbose", is_flag=True, help="Verbose mode.")
@option(
    "-l", "--limit", type=int, help="Limit the max number of objects in the output."
)
@pass_context
def list_replication_queue_command(ctx: Context, **kwargs: Any) -> None:
    """
    List replication queue tasks.
    """
    logging.info(get_replication_queue_tasks(ctx, **kwargs, format_="Vertical"))


@replication_queue_group.command("delete")
@option(
    "--failed",
    is_flag=True,
    help="Delete only failed replication queue tasks (tasks with non-empty exception).",
)
@option(
    "--error",
    "--exception",
    "exception",
    help="Filter replication queue tasks to delete by the specified exception.",
)
@option(
    "--executing", is_flag=True, help="Delete only executing replication queue tasks."
)
@option(
    "--age",
    "--min-age",
    "min_age",
    type=TimeSpanParamType(),
    help="Delete only replication queue tasks that were created at least the specified amount of time ago.",
)
@option(
    "--type",
    "type_",
    help="Filter replication queue tasks to delete by the specified type."
    " Multiple values can be specified through a comma.",
)
@option(
    "--exclude-type",
    help="Filter replication queue tasks to not delete by the specified type."
    " Multiple values can be specified through a comma.",
)
@option(
    "-d",
    "--database",
    help="Filter replication queue tasks to delete by the specified database."
    " Multiple values can be specified through a comma.",
)
@option(
    "-t",
    "--table",
    help="Filter replication queue tasks to delete by the specified table."
    " Multiple values can be specified through a comma.",
)
@pass_context
def delete_command(ctx: Context, **kwargs: Any) -> None:
    """
    Delete replication queue tasks.
    """
    tasks = get_replication_queue_tasks(ctx, **kwargs, verbose=True, format_="JSON")[
        "data"
    ]
    for table, tasks in group_tasks_by_table(tasks).items():
        database, table = table

        logging.info("Detaching table `{}`.`{}`", database, table)
        execute_query(
            ctx,
            f"""DETACH TABLE `{database}`.`{table}`""",
            timeout=300,
            echo=True,
            format_=None,
        )

        for task in tasks:
            zk_path = task["zk_path"]
            logging.info("Deleting task from ZooKeeper: {}", zk_path)
            delete_zk_node(ctx, zk_path)

        logging.info("Attaching table `{}`.`{}`", database, table)
        execute_query(
            ctx,
            f"""ATTACH TABLE `{database}`.`{table}`""",
            timeout=300,
            echo=True,
            format_=None,
        )


def get_replication_queue_tasks(
    ctx: Context,
    *,
    on_cluster: Optional[bool] = None,
    failed: Optional[bool] = None,
    exception: Optional[str] = None,
    executing: Optional[bool] = None,
    min_age: Optional[Any] = None,
    type_: Optional[str] = None,
    exclude_type: Optional[str] = None,
    database: Optional[str] = None,
    table: Optional[str] = None,
    partition_id: Optional[str] = None,
    min_postpone_count: Optional[int] = None,
    max_postpone_count: Optional[int] = None,
    verbose: Optional[bool] = None,
    limit: Optional[int] = None,
    format_: Optional[Union[str, OutputFormat]] = None,
) -> Any:
    cluster = get_cluster_name(ctx) if on_cluster else None
    query = """
    SELECT
    {% if cluster %}
        hostName() "host",
    {% endif %}
        database,
        table,
        position,
        node_name,
    {% if verbose %}
        replica_path || '/queue/' || node_name "zk_path",
    {% endif %}
        type,
        is_currently_executing,
        source_replica,
        parts_to_merge,
        new_part_name,
        splitByChar('_', new_part_name, 1)[1] "partition_id",
        create_time,
        last_attempt_time "attempt_time",
        last_exception "exception",
        concat('time: ', toString(last_postpone_time), ', number: ', toString(num_postponed), ', reason: ', postpone_reason) "postpone"
    {% if cluster %}
    FROM clusterAllReplicas({{ cluster }}, system.replication_queue)
    {% else %}
    FROM system.replication_queue
    {% endif %}
    {% if verbose %}
    JOIN system.replicas USING (database, table)
    {% endif %}
    WHERE 1
    {% if database %}
      AND database {{ format_str_match(database) }}
    {% endif %}
    {% if table %}
      AND table {{ format_str_match(table) }}
    {% endif %}
    {% if partition_id -%}
      AND partition_id {{ format_str_match(partition_id) }}
    {% endif -%}
    {% if failed %}
      AND last_exception != ''
    {% endif %}
    {% if exception %}
      AND last_exception {{ format_str_match(exception) }}
    {% endif %}
    {% if executing %}
      AND is_currently_executing
    {% endif %}
    {% if min_age %}
      AND create_time <= now() - toIntervalSecond({{ min_age }})
    {% endif %}
    {% if type %}
      AND type {{ format_str_match(type) }}
    {% endif %}
    {% if exclude_type %}
      AND type NOT {{ format_str_match(exclude_type) }}
    {% endif %}
    {% if min_postpone_count -%}
      AND num_postponed >= {{ min_postpone_count }}
    {% endif %}
    {% if max_postpone_count -%}
      AND num_postponed <= {{ max_postpone_count }}
    {% endif %}
    ORDER BY database, table, position
    {% if limit %}
    LIMIT {{ limit }}
    {% endif %}
    """
    return execute_query(
        ctx,
        query,
        cluster=cluster,
        database=database,
        table=table,
        partition_id=partition_id,
        failed=failed,
        exception=exception,
        executing=executing,
        min_age=min_age.total_seconds() if min_age else None,
        type=type_,
        exclude_type=exclude_type,
        min_postpone_count=min_postpone_count,
        max_postpone_count=max_postpone_count,
        verbose=verbose,
        limit=limit,
        format_=format_,
    )


def group_tasks_by_table(tasks: Any) -> Any:
    result = defaultdict(list)
    for task in tasks:
        result[(task["database"], task["table"])].append(task)
    return result
