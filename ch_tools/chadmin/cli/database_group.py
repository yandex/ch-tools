from click import Context
from cloup import argument, group, option, option_group, pass_context
from cloup.constraints import RequireAtLeast

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.cli.database_metadata import (
    DatabaseMetadata,
    parse_database_from_metadata,
)
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.chadmin.internal.zookeeper import (
    check_zk_node,
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


def is_first_replica_migrate(ctx: Context, migrating_database_name: str) -> bool:
    first_replica_database_name = (
        f"/clickhouse/{migrating_database_name}/first_replica_database_name"
    )

    if not check_zk_node(ctx, first_replica_database_name):
        logging.info(
            "Node for {} not exists. Finish checking.", migrating_database_name
        )
        return True
    logging.info("Node for {} exists. Continue checking.", migrating_database_name)

    db_in_node = get_zk_node(ctx, first_replica_database_name)
    logging.info(
        "migrating_database_name={}, db_in_node={}", migrating_database_name, db_in_node
    )

    assert db_in_node == migrating_database_name
    return False


def detach_dbs(ctx, dbs):
    for db in dbs:
        query = f"""
            DETACH DATABASE {db}
        """
        execute_query(
            ctx,
            query,
            echo=True,
        )


def update_zk_for_migrate(ctx: Context, metadata_non_repl_db: DatabaseMetadata) -> None:
    first_replica_database_name = (
        f"/clickhouse/{metadata_non_repl_db.database_name}/first_replica_database_name"
    )

    data = get_zk_node(ctx, first_replica_database_name)

    if data != metadata_non_repl_db.database_name:
        update_zk_nodes(
            ctx, [first_replica_database_name], metadata_non_repl_db.database_name
        )
    else:
        logging.info(
            "first_replica_database_name was migrated early. Skip on current replica."
        )

    for replica_path in list_zk_nodes(
        ctx, f"/clickhouse/{metadata_non_repl_db.database_name}/replicas"
    ):
        logging.info("Update replica: {}", replica_path)
        replica_data = get_zk_node(ctx, replica_path)

        prefix = replica_data.split(":")
        new_data = f"{prefix[0]}:{prefix[1]}:{metadata_non_repl_db.database_uuid}"

        update_zk_nodes(ctx, [replica_path], new_data)


def remove_temp_db(ctx: Context, metadata_temp_db: DatabaseMetadata) -> None:
    metadata_temp_db.update_metadata_file()
    query = f"""
        ATTACH DATABASE {metadata_temp_db.database_name}
    """
    execute_query(
        ctx,
        query,
        echo=True,
    )

    query = f"""
        DROP DATABASE {metadata_temp_db.database_name} SYNC
    """
    execute_query(
        ctx,
        query,
        echo=True,
    )


def migrate_as_first_replica(ctx, database, temp_db):
    query = """
        CREATE DATABASE {temp_db} ON CLUSTER '{{cluster}}' ENGINE = Replicated('/clickhouse/{database}', '{{shard}}', '{{replica}}')
    """.format(
        temp_db=temp_db,
        database=database,
    )
    execute_query(
        ctx,
        query,
        echo=True,
    )

    metadata_non_repl_db = parse_database_from_metadata(database)
    metadata_temp_db = parse_database_from_metadata(temp_db)

    original_engine = metadata_non_repl_db.database_engine

    detach_dbs(ctx, dbs=[database, temp_db])

    metadata_non_repl_db.set_engine_from(metadata_temp_db)
    metadata_non_repl_db.update_metadata_file()

    update_zk_for_migrate(ctx, metadata_non_repl_db)

    query = f"""
        ATTACH DATABASE {database}
    """
    execute_query(
        ctx,
        query,
        echo=True,
    )

    metadata_temp_db.database_engine = original_engine
    metadata_temp_db.update_metadata_file()

    remove_temp_db(ctx, metadata_temp_db)


def migrate_as_non_first_replica(ctx, database, temp_db):
    metadata_non_repl_db = parse_database_from_metadata(database)
    metadata_temp_db = parse_database_from_metadata(temp_db)

    original_engine = metadata_non_repl_db.database_engine

    detach_dbs(ctx, dbs=[database, temp_db])

    metadata_non_repl_db.set_engine_from(metadata_temp_db)
    metadata_non_repl_db.update_metadata_file()

    query = f"""
        ATTACH DATABASE {database}
    """
    execute_query(
        ctx,
        query,
        echo=True,
    )

    metadata_temp_db.database_engine = original_engine
    metadata_temp_db.update_metadata_file()

    remove_temp_db(ctx, metadata_temp_db)


@database_group.command("migrate")
@option("-d", "--database")
@pass_context
def migrate_engine_command(ctx, database):
    temp_db = f"temp_migrate_{database}"

    if is_first_replica_migrate(ctx, database):
        migrate_as_first_replica(ctx, database, temp_db)
    else:
        migrate_as_non_first_replica(ctx, database, temp_db)
