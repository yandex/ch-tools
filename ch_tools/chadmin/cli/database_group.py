import sys

from cloup import argument, group, option, option_group, pass_context
from cloup.constraints import RequireAtLeast

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.migration import (
    create_database_nodes,
    create_database_replica,
    create_temp_db,
    is_database_exists,
    migrate_as_first_replica,
    migrate_as_non_first_replica,
)
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.clickhouse.config import get_cluster_name


@group("database", cls=Chadmin)
def database_group():
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
def get_database_command(ctx, database, active_parts):
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
def list_databases_command(ctx, **kwargs):
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
    ctx,
    _all,
    database,
    exclude_database,
    on_cluster,
    dry_run,
):
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
    ctx, database=None, exclude_database=None, active_parts=None, format_=None
):
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
@pass_context
def migrate_engine_command(ctx, database):
    temp_db = f"temp_migrate_{database}"

    try:
        if not is_database_exists(ctx, database):
            logging.error("Database {} does not exists, skip migrating", database)
            sys.exit(1)

        first_replica = True
        try:
            # create_temp_db(ctx, database, temp_db)
            create_database_nodes(ctx, database)
        except Exception as ex:
            logging.info("create_database_nodes failed with ex={}", ex)

            # exception Node exists
            non_first_replica_errors = [
                "REPLICA_ALREADY_EXISTS",
                "DATABASE_ALREADY_EXISTS",
            ]
            if not any(
                suitable_error in str(ex) for suitable_error in non_first_replica_errors
            ):
                raise

            first_replica = False

        # create replica
        create_database_replica(ctx, database)

        if first_replica:
            logging.info("migrate as first replica")
            migrate_as_first_replica(ctx, database)
        else:
            logging.info("migrate as non first replica")
            migrate_as_non_first_replica(ctx, database, temp_db)

    except Exception as ex:
        logging.error("Got exception: {}", ex)
        sys.exit(1)
