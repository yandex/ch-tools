from cloup import argument, group, option, option_group, pass_context
from cloup.constraints import RequireAtLeast

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.cli.database_metadata import parse_database_metadata
from ch_tools.chadmin.internal.clickhouse_disks import CLICKHOUSE_METADATA_PATH
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.chadmin.internal.zookeeper import (
    get_zk_node,
    list_zk_nodes,
    update_zk_nodes,
)
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
@option("-d", "--database")
@pass_context
def migrate_engine_command(ctx, database):
    query = """
        CREATE DATABASE temp_db ENGINE = Replicated('/clickhouse/{database}', '{{shard}}', '{{replica}}')
    """.format(
        database=database
    )

    execute_query(
        ctx,
        query,
        echo=True,
    )

    target_metadata_path = CLICKHOUSE_METADATA_PATH + f"/{database}.sql"
    temp_metadata_path = CLICKHOUSE_METADATA_PATH + "/temp_db.sql"

    metadata_non_repl_db = parse_database_metadata(target_metadata_path)
    metadata_temp_db = parse_database_metadata(temp_metadata_path)

    query = f"""
        DETACH DATABASE {database}
    """
    execute_query(
        ctx,
        query,
        echo=True,
    )

    query = """
        DETACH DATABASE temp_db
    """
    execute_query(
        ctx,
        query,
        echo=True,
    )

    metadata_non_repl_db.set_engine_from(metadata_temp_db)
    metadata_non_repl_db.update_metadata_file(target_metadata_path)

    # target_zk_data = get_zk_node(ctx, f"/clickhouse/{database}/first_replica_database_name")
    update_zk_nodes(
        ctx, [f"/clickhouse/{database}/first_replica_database_name"], database
    )

    # target_zk_data = get_zk_node(ctx, f"/clickhouse/{database}/first_replica_database_name")
    # logging.info("after target_zk_data first_replica_database_name={}", target_zk_data)

    for replica_path in list_zk_nodes(ctx, f"/clickhouse/{database}/replicas"):
        replica_data = get_zk_node(ctx, replica_path)

        prefix = replica_data.split(":")
        new_data = f"{prefix[0]}:{prefix[1]}:{metadata_non_repl_db.database_uuid}"

        update_zk_nodes(ctx, [replica_path], new_data)

    query = f"""
        ATTACH DATABASE {database}
    """
    execute_query(
        ctx,
        query,
        echo=True,
    )
