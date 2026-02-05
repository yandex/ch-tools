from typing import Any

from click import Context
from cloup import argument, group, option, option_group, pass_context
from cloup.constraints import RequireAtLeast

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.cli.database_metadata import (
    DatabaseEngine,
    parse_database_metadata,
)
from ch_tools.chadmin.internal.database import is_database_exists, list_databases
from ch_tools.chadmin.internal.database_replica import (
    _restore_replica_fallback,
    restore_replica_with_system_command,
    supports_system_restore_database_replica,
)
from ch_tools.chadmin.internal.migration import (
    migrate_database_to_atomic,
    migrate_database_to_replicated,
)
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.clickhouse.config import get_cluster_name


@group("database", cls=Chadmin)
def database_group() -> None:
    """Commands to manage databases."""
    pass


@database_group.command("get")
@argument("database")
@option(
    "--active",
    "--active-parts",
    "active_parts",
    is_flag=True,
    help="Account only active data parts.",
)
@pass_context
def get_database_command(ctx: Context, database: str, active_parts: bool) -> None:
    logging.info(
        list_databases(
            ctx, database=database, active_parts=active_parts, format_="Vertical"
        )
    )


@database_group.command("list")
@option(
    "-d",
    "--database",
    help="Filter in databases to output by the specified database name pattern."
    " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
)
@option(
    "--exclude-database",
    "exclude_database_pattern",
    help="Filter out databases to output by the specified database name pattern."
    " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
)
@option(
    "--active",
    "--active-parts",
    "active_parts",
    is_flag=True,
    help="Account only active data parts.",
)
@pass_context
def list_databases_command(ctx: Context, **kwargs: Any) -> None:
    logging.info(list_databases(ctx, **kwargs, format_="PrettyCompact"))


@database_group.command("delete")
@option_group(
    "Database selection options",
    option("-a", "--all", "_all", is_flag=True, help="Delete all databases."),
    option(
        "-d",
        "--database",
        help="Filter in databases to delete by the specified database name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-database",
        "exclude_database_pattern",
        help="Filter out databases to delete by the specified database name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    constraint=RequireAtLeast(1),
)
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Delete databases on all hosts of the cluster.",
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def delete_databases_command(
    ctx: Context,
    _all: bool,
    database: str,
    exclude_database_pattern: str,
    on_cluster: bool,
    dry_run: bool,
) -> None:
    cluster = get_cluster_name(ctx) if on_cluster else None

    for d in list_databases(
        ctx,
        database=database,
        exclude_database_pattern=exclude_database_pattern,
    ):
        query = """
            DROP DATABASE `{{ database }}`
            {% if cluster %}
            ON CLUSTER '{{ cluster }}'
            {% endif %}
            """
        execute_query(
            ctx,
            query,
            database=d["database"],
            cluster=cluster,
            echo=True,
            dry_run=dry_run,
        )


@database_group.command("migrate")
@option("-d", "--database", required=True)
@option(
    "-e",
    "--engine",
    required=True,
    help="Database engine: Atomic or Replicated. After migration to Atomic need manually delete zookeeper nodes.",
)
@option(
    "--clean-zookeeper",
    is_flag=True,
    default=False,
    help="Remove zookeeper nodes related with Replicated database.",
)
@pass_context
def migrate_engine_command(
    ctx: Context, database: str, engine: str, clean_zookeeper: bool
) -> None:
    if not is_database_exists(ctx, database):
        raise RuntimeError(f"Database {database} does not exists, skip migrating")

    if DatabaseEngine.from_str(engine) == DatabaseEngine.REPLICATED:
        migrate_database_to_replicated(ctx, database)
    else:
        migrate_database_to_atomic(ctx, database, clean_zookeeper)


@database_group.command("restore-replica")
@option("-d", "--database", required=True)
@pass_context
def restore_replica_command(ctx: Context, database: str) -> None:
    """
    Restore database replica using SYSTEM RESTORE DATABASE REPLICA command.

    For ClickHouse >= 25.8, uses the built-in SYSTEM RESTORE DATABASE REPLICA command.
    For older versions, falls back to manual ZooKeeper structure creation.
    """

    # Validation checks
    if not is_database_exists(ctx, database):
        raise RuntimeError(f"Database {database} does not exists, skip restore")

    db_metadata = parse_database_metadata(database)

    if not db_metadata.database_engine.is_replicated():
        raise RuntimeError(f"Database {database} is not Replicated, stop restore")

    # Try using SYSTEM RESTORE DATABASE REPLICA for CH >= 25.8
    if supports_system_restore_database_replica(ctx):
        try:
            restore_replica_with_system_command(ctx, database)
            return
        except Exception as e:
            logging.error(f"SYSTEM RESTORE DATABASE REPLICA failed: {e}")
            raise

    # Fallback for older versions
    logging.info("Using fallback method for ClickHouse < 25.8")
    _restore_replica_fallback(ctx, database, db_metadata.zookeeper_path)
