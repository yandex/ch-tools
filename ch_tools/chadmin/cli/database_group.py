import sys
from typing import Any, Optional

from click import Context
from cloup import argument, group, option, option_group, pass_context
from cloup.constraints import RequireAtLeast
from kazoo.exceptions import NodeExistsError

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.cli.database_metadata import (
    DatabaseEngine,
    parse_database_from_metadata,
)
from ch_tools.chadmin.internal.migration import (
    create_database_nodes,
    is_database_exists,
    migrate_database_to_atomic,
    migrate_database_to_replicated,
    restore_replica,
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
        get_databases(
            ctx, database=database, active_parts=active_parts, format_="Vertical"
        )
    )


@database_group.command("list")
@option("-d", "--database")
@option("--exclude-database")
@option(
    "--active",
    "--active-parts",
    "active_parts",
    is_flag=True,
    help="Account only active data parts.",
)
@pass_context
def list_databases_command(ctx: Context, **kwargs: Any) -> None:
    logging.info(get_databases(ctx, **kwargs, format_="PrettyCompact"))


@database_group.command("delete")
@option_group(
    "Database selection options",
    option("-a", "--all", "_all", is_flag=True, help="Delete all databases."),
    option("-d", "--database"),
    option("--exclude-database"),
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
    exclude_database: str,
    on_cluster: bool,
    dry_run: bool,
) -> None:
    cluster = get_cluster_name(ctx) if on_cluster else None

    for d in get_databases(
        ctx, database=database, exclude_database=exclude_database, format_="JSON"
    )["data"]:
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


def get_databases(
    ctx: Context,
    database: Optional[str] = None,
    exclude_database: Optional[str] = None,
    active_parts: Optional[bool] = None,
    format_: Optional[str] = None,
) -> Any:
    query = """
        SELECT
            database,
            engine,
            tables,
            formatReadableSize(bytes_on_disk) "disk_size",
            partitions,
            parts,
            rows
        FROM (
            SELECT
                name "database",
                engine
            FROM system.databases
        ) q1
        ALL LEFT JOIN (
            SELECT
                database,
                count() "tables",
                sum(bytes_on_disk) "bytes_on_disk",
                sum(partitions) "partitions",
                sum(parts) "parts",
                sum(rows) "rows"
            FROM (
                SELECT
                    database,
                    name "table"
                FROM system.tables
            ) q2_1
            ALL LEFT JOIN (
                SELECT
                    database,
                    table,
                    uniq(partition) "partitions",
                    count() "parts",
                    sum(rows) "rows",
                    sum(bytes_on_disk) "bytes_on_disk"
                FROM system.parts
        {% if active_parts %}
                WHERE active
        {% endif %}
                GROUP BY database, table
            ) q2_2
            USING database, table
            GROUP BY database
        ) q2
        USING database
        {% if database %}
        WHERE database {{ format_str_match(database) }}
        {% else %}
        WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
        {% endif %}
        {% if exclude_database %}
          AND database != '{{ exclude_database }}'
        {% endif %}
        ORDER BY database
        """
    return execute_query(
        ctx,
        query,
        database=database,
        exclude_database=exclude_database,
        active_parts=active_parts,
        format_=format_,
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
    try:
        if not is_database_exists(ctx, database):
            logging.error("Database {} does not exists, skip migrating", database)
            sys.exit(1)

        if DatabaseEngine.from_str(engine) == DatabaseEngine.REPLICATED:
            migrate_database_to_replicated(ctx, database)
        else:
            migrate_database_to_atomic(ctx, database, clean_zookeeper)

    except Exception as ex:
        logging.error("Got exception: type={}, ex={}", type(ex), ex)
        sys.exit(1)


@database_group.command("restore-replica")
@option("-d", "--database", required=True)
@pass_context
def restore_replica_command(ctx: Context, database: str) -> None:
    try:
        if not is_database_exists(ctx, database):
            logging.error("Database {} does not exists, skip restore", database)
            sys.exit(1)

        db_metadata = parse_database_from_metadata(database)

        if not db_metadata.database_engine.is_replicated():
            logging.error("Database {} is not Replicated, stop restore", database)
            sys.exit(1)

        first_replica = True
        try:
            create_database_nodes(
                ctx,
                database_name=database,
                db_replica_path=db_metadata.replica_path,
            )
        except NodeExistsError as ex:
            logging.info(
                "create_database_nodes failed with NodeExistsError. {}, type={}. Restore as second replica",
                ex,
                type(ex),
            )
            first_replica = False
        except Exception as ex:
            logging.info("create_database_nodes failed with ex={}", type(ex))
            raise ex

        restore_replica(
            ctx,
            database,
            first_replica=first_replica,
            db_replica_path=db_metadata.replica_path,
        )
    except Exception as ex:
        logging.error("Got exception: type={}, ex={}", type(ex), ex)
        sys.exit(1)
