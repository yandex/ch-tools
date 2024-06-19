from collections import OrderedDict
from typing import List

from click import ClickException
from cloup import (
    Context,
    argument,
    constraint,
    group,
    option,
    option_group,
    pass_context,
)
from cloup.constraints import AnySet, If, RequireAtLeast, accept_none, require_all

from ch_tools.chadmin.internal.table_replica import (
    get_table_replica,
    list_table_replicas,
    restart_table_replica,
    restore_table_replica,
)
from ch_tools.common import logging
from ch_tools.common.cli.formatting import print_response
from ch_tools.common.clickhouse.client import ClickhouseError
from ch_tools.common.clickhouse.config import get_cluster_name
from ch_tools.common.process_pool import WorkerTask, execute_tasks_in_parallel


@group("replica")
def replica_group():
    """Commands to manage table replicas."""
    pass


@replica_group.command("get")
@argument("database_name", metavar="DATABASE")
@argument("table_name", metavar="TABLE")
@pass_context
def get_replica_command(ctx, database_name, table_name):
    """
    Get table replica.
    """
    print_response(ctx, get_table_replica(ctx, database_name, table_name))


@replica_group.command("list")
@option(
    "-d",
    "--database",
    "database_pattern",
    help="Filter in replicas to output by the specified database name pattern."
    " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
)
@option(
    "--exclude-database",
    "exclude_database_pattern",
    help="Filter out replicas to output by the specified database name pattern."
    " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
)
@option(
    "-t",
    "--table",
    "table_pattern",
    help="Filter in replicas to output by the specified table name."
    " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
)
@option(
    "--exclude-table",
    "exclude_table_pattern",
    help="Filter out replicas to output by the specified table name."
    " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
)
@option(
    "--read-only",
    "is_readonly",
    is_flag=True,
    help="Filter in replicas in read-only state only.",
)
@option(
    "-l",
    "--limit",
    type=int,
    default=1000,
    help="Limit the max number of objects in the output.",
)
@pass_context
def list_command(ctx, **kwargs):
    """
    List table replicas.
    """

    def _table_formatter(item):
        return OrderedDict(
            (
                ("database", item["database"]),
                ("table", item["table"]),
                ("zookeeper_path", item["zookeeper_path"]),
                ("replica_name", item["replica_name"]),
                ("is_readonly", item["is_readonly"]),
                ("absolute_delay", item["absolute_delay"]),
                ("queue_size", item["queue_size"]),
                (
                    "active_replicas",
                    f"{item['active_replicas']} / {item['total_replicas']}",
                ),
            )
        )

    table_replicas = list_table_replicas(ctx, verbose=True, **kwargs)
    print_response(
        ctx,
        table_replicas,
        default_format="table",
        table_formatter=_table_formatter,
    )


@replica_group.command("restart")
@argument("database_name", metavar="DATABASE", required=False)
@argument("table_name", metavar="TABLE", required=False)
@constraint(If("database_name", then=require_all), ["table_name"])
@option_group(
    "Replica selection options",
    option(
        "-a",
        "--all",
        "_all",
        is_flag=True,
        help="Filter in all replicas.",
    ),
    option(
        "-d",
        "--database",
        "database_pattern",
        help="Filter in replicas to restore by the specified database name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-database",
        "exclude_database_pattern",
        help="Filter out replicas to restore by the specified database name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "-t",
        "--table",
        "table_pattern",
        help="Filter in replicas to restore by the specified table name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-table",
        "exclude_table_pattern",
        help="Filter out replicas to restore by the specified table name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    constraint=If(
        AnySet("database_name", "table_name"),
        then=accept_none,
        else_=RequireAtLeast(1),
    ),
)
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Restart replicas on all hosts of the cluster.",
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def restart_replica_command(
    ctx,
    _all,
    database_name,
    table_name,
    on_cluster,
    dry_run,
    **kwargs,
):
    """
    Restart one or several table replicas.
    """
    cluster = get_cluster_name(ctx) if on_cluster else None

    if database_name and table_name:
        replicas = [get_table_replica(ctx, database_name, table_name)]
    else:
        replicas = list_table_replicas(ctx, **kwargs)

    for replica in replicas:
        restart_table_replica(
            ctx,
            replica["database"],
            replica["table"],
            cluster=cluster,
            dry_run=dry_run,
        )


@replica_group.command("restore")
@argument("database_name", metavar="DATABASE", required=False)
@argument("table_name", metavar="TABLE", required=False)
@constraint(If("database_name", then=require_all), ["table_name"])
@option_group(
    "Replica selection options",
    option(
        "-a",
        "--all",
        "_all",
        is_flag=True,
        help="Filter in all replicas.",
    ),
    option(
        "-d",
        "--database",
        "database_pattern",
        help="Filter in replicas to restore by the specified database name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-database",
        "exclude_database_pattern",
        help="Filter out replicas to restore by the specified database name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "-t",
        "--table",
        "table_pattern",
        help="Filter in replicas to restore by the specified table name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-table",
        "exclude_table_pattern",
        help="Filter out replicas to restore by the specified table name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    constraint=If(
        AnySet("database_name", "table_name"),
        then=accept_none,
        else_=RequireAtLeast(1),
    ),
)
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Restore replicas on all hosts of the cluster.",
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@option("-w", "--workers", default=4, help="Number of workers.")
@option("-k", "--keep-going", is_flag=True, help="Do not stop on the first error.")
@pass_context
def restore_command(
    ctx,
    _all,
    database_name,
    table_name,
    on_cluster,
    dry_run,
    workers,
    keep_going,
    **kwargs,
):
    """
    Restore one or several table replicas.
    """
    cluster = get_cluster_name(ctx) if on_cluster else None

    if database_name and table_name:
        replica = get_table_replica(ctx, database_name, table_name)
        if not replica["is_readonly"]:
            raise ClickException(
                f"Replicated table `{database_name}`.`{table_name}` must be in read-only in order to perform restore."
            )
        replicas = [replica]
    else:
        replicas = list_table_replicas(ctx, is_readonly=True, **kwargs)

    tasks: List[WorkerTask] = []
    for replica in replicas:
        tasks.append(
            WorkerTask(
                f"restore_replica_{replica['database']}.{replica['table']}",
                restore_replica,
                {
                    "ctx": ctx,
                    "database": replica["database"],
                    "table": replica["table"],
                    "cluster": cluster,
                    "dry_run": dry_run,
                },
            )
        )
    execute_tasks_in_parallel(tasks, max_workers=workers, keep_going=keep_going)


def restore_replica(
    ctx: Context, database: str, table: str, cluster: str, dry_run: bool
) -> None:
    try:
        restore_table_replica(
            ctx,
            database,
            table,
            cluster=cluster,
            dry_run=dry_run,
        )
    except ClickhouseError as e:
        msg = e.response.text.strip()
        if "NO_ZOOKEEPER" in msg or "Session expired" in msg:
            logging.warning(
                'Failed to restore replica with error "{}", attempting to recover by restarting replica and retrying restore',
                msg,
            )
            restart_table_replica(
                ctx,
                database,
                table,
                cluster=cluster,
                dry_run=dry_run,
            )
            restore_table_replica(
                ctx,
                database,
                table,
                cluster=cluster,
                dry_run=dry_run,
            )
        elif "Replica path is present" in msg:
            logging.warning(
                'Failed to restore replica with error "{}", attempting to recover by restarting replica',
                msg,
            )
            restart_table_replica(
                ctx,
                database,
                table,
                cluster=cluster,
                dry_run=dry_run,
            )
        else:
            raise
