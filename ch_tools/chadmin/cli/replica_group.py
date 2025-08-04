from collections import OrderedDict
from typing import Any, Dict, List, Optional

from click import ClickException, Context
from cloup import argument, constraint, group, option, option_group, pass_context
from cloup.constraints import AnySet, If, RequireAtLeast, accept_none, require_all

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.table_replica import (
    get_table_replica,
    list_table_replicas,
    restart_table_replica,
    restore_replica,
    system_table_drop_replica_by_zk_path,
)
from ch_tools.chadmin.internal.zookeeper import get_table_shared_id
from ch_tools.chadmin.internal.zookeeper_clean import delete_zero_copy_locks
from ch_tools.common import logging
from ch_tools.common.cli.formatting import print_response
from ch_tools.common.clickhouse.config import get_cluster_name
from ch_tools.common.process_pool import WorkerTask, execute_tasks_in_parallel


@group("replica", cls=Chadmin)
def replica_group() -> None:
    """Commands to manage table replicas."""
    pass


@replica_group.command("get")
@argument("database_name", metavar="DATABASE")
@argument("table_name", metavar="TABLE")
@pass_context
def get_replica_command(ctx: Context, database_name: str, table_name: str) -> None:
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
def list_command(ctx: Context, **kwargs: Any) -> None:
    """
    List table replicas.
    """

    def _table_formatter(item: Dict[str, Any]) -> OrderedDict:
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
    option(
        "--read-only",
        "is_readonly",
        is_flag=True,
        help="Filter in replicas in read-only state only.",
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
    ctx: Context,
    _all: bool,
    database_name: Optional[str],
    table_name: Optional[str],
    on_cluster: bool,
    dry_run: bool,
    **kwargs: Any,
) -> None:
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
    ctx: Context,
    _all: bool,
    database_name: Optional[str],
    table_name: Optional[str],
    on_cluster: bool,
    dry_run: bool,
    workers: int,
    keep_going: bool,
    **kwargs: Any,
) -> None:
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


@replica_group.command("drop")
@argument("replica", required=True)
@argument("zookeeper_path", required=True)
@option(
    "-t",
    "--table-uuid",
    "table_uuid",
    default=None,
    help=(
        "UUID of a table to clean. Will get it from 'table_shared_id' node by default."
    ),
)
@option(
    "--zero-copy-path",
    "zero_copy_path",
    default=None,
    help=(
        "Path to zero-copy related data in ZooKeeper."
        "Will use 'remote_fs_zero_copy_zookeeper_path' value from ClickHouse by default."
    ),
)
@option(
    "--disk_type",
    "disk_type",
    default="s3",
    help=(
        "Object storage disk type from ClickHouse."
        "Examples are s3, hdfs, azure_blob_storage, local_blob_storage..."
    ),
)
@option(
    "-n",
    "--dry-run",
    "dry_run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def drop_command(
    ctx: Context,
    replica: str,
    zookeeper_path: str,
    table_uuid: str,
    zero_copy_path: Optional[str] = None,
    disk_type: str = "s3",
    dry_run: bool = False,
) -> None:
    """
    Drop replica and zero-copy locks in ZooKeeper.
    """
    try:
        system_table_drop_replica_by_zk_path(ctx, replica, zookeeper_path, dry_run)
    except:
        logging.warning(
            "Drop replica failed, zero-copy locks are not deleted. "
            "Retry this command or use 'zookeeper cleanup-zero-copy-locks' instead."
        )
        raise

    table_uuid = table_uuid or get_table_shared_id(ctx, zookeeper_path)

    if not table_uuid:
        logging.info(
            "Can't find table_shared_id to clean zero-copy locks. Cleaning is skipped."
        )

    delete_zero_copy_locks(
        ctx,
        zero_copy_path=zero_copy_path,
        disk_type=disk_type,
        table_uuid=table_uuid,
        replica_name=replica,
        dry_run=dry_run,
    )
