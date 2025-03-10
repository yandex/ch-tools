import grp
import os
import pwd

from click import Context
from cloup import argument, group, option, option_group, pass_context
from cloup.constraints import RequireAtLeast

from ch_tools.chadmin.cli import metadata
from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.cli.database_metadata import (
    DatabaseMetadata,
    parse_database_from_metadata,
)
from ch_tools.chadmin.internal.clickhouse_disks import CLICKHOUSE_PATH
from ch_tools.chadmin.internal.system import get_version, match_str_ch_version
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.chadmin.internal.zookeeper import (
    check_zk_node,
    get_zk_node,
    list_zk_nodes,
    update_zk_nodes,
)
from ch_tools.common import logging
from ch_tools.common.clickhouse.client.query_output_format import OutputFormat
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


def update_zk_tables_metadata(ctx, metadata_non_repl_db, tables):
    for metadata_path in list_zk_nodes(
        ctx, f"/clickhouse/{metadata_non_repl_db.database_name}/metadata"
    ):
        data = get_zk_node(ctx, metadata_path)

        logging.info(
            "found node for table. path={}, contains data={}", metadata_path, data
        )
        table_name = metadata_path.split("/")[-1]
        target_metadata = tables[table_name]

        logging.info("new metadata for node:\n{}\n===", target_metadata)

        update_zk_nodes(ctx, [metadata_path], target_metadata)

        data = get_zk_node(ctx, metadata_path)
        logging.info("after update data:\n{}\n===", data)


def update_zk_for_migrate(
    ctx: Context, metadata_non_repl_db: DatabaseMetadata, tables: dict
) -> None:
    first_replica_database_name = (
        f"/clickhouse/{metadata_non_repl_db.database_name}/first_replica_database_name"
    )

    data = get_zk_node(ctx, first_replica_database_name)

    logging.info("first_replica_database_name contains: {}", data)

    if data != metadata_non_repl_db.database_name:
        logging.info("update zk path {}", first_replica_database_name)
        update_zk_nodes(
            ctx, [first_replica_database_name], metadata_non_repl_db.database_name
        )

        update_zk_tables_metadata(ctx, metadata_non_repl_db, tables)
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


def create_tables_from_migrating_database(ctx, migrating_database, temp_db):
    query = f"""
        SELECT name, create_table_query, metadata_path FROM system.tables WHERE database='{migrating_database}'
    """
    rows = execute_query(ctx, query, echo=True, format_=OutputFormat.JSON)

    tables = {}

    for row in rows["data"]:
        name = row["name"]
        metadata_path = row["metadata_path"]
        create_table_query = row["create_table_query"]

        logging.info(
            "name={}, metadata_path={}, create_table_query=[{}]",
            name,
            metadata_path,
            create_table_query,
        )

        create_table_query = create_table_query.replace(migrating_database, temp_db)
        logging.info("after replacing create_table_query=[{}]", create_table_query)

        execute_query(
            ctx,
            create_table_query,
            echo=True,
        )

        if match_str_ch_version(get_version(ctx), "25.1"):
            metadata_path = CLICKHOUSE_PATH + "/" + metadata_path

        with open(metadata_path, "r", encoding="utf-8") as metadata_file:
            # todo
            tables[name] = metadata_file.read()
            logging.info("add from file with metadata to tables: {}", tables[name])

    query = f"""
        SYSTEM SYNC DATABASE REPLICA {temp_db}
    """
    execute_query(
        ctx,
        query,
        echo=True,
        format_=None,
    )
    logging.info("total tables: {}", tables)
    return tables


def get_tables_local_metadata_path(ctx, database_name):
    query = f"""
        SELECT database, name, uuid, create_table_query, metadata_path FROM system.tables WHERE database='{database_name}'
    """
    rows = execute_query(ctx, query, echo=True, format_=OutputFormat.JSON)

    tables = rows["data"]

    for row in rows["data"]:
        logging.info("got row={}", row)
        name = row["name"]
        metadata_path = row["metadata_path"]
        create_table_query = row["create_table_query"]

        logging.info(
            "name={}, metadata_path={}, create_table_query={}",
            name,
            metadata_path,
            create_table_query,
        )

        # wtf?
        query = f"""
            DETACH TABLE {database_name}.{name}
        """
        execute_query(
            ctx,
            query,
            echo=True,
            format_=None,
        )

        # if match_str_ch_version(get_version(ctx), "25.1"):
        #     metadata_path = CLICKHOUSE_PATH + "/" + metadata_path

    logging.info("Database {} contains tables: {}", database_name, tables)
    return tables


def migrate_as_first_replica(ctx, migrating_database, temp_db):
    query = """
        CREATE DATABASE {temp_db} ON CLUSTER '{{cluster}}' ENGINE = Replicated('/clickhouse/{database}', '{{shard}}', '{{replica}}')
    """.format(
        temp_db=temp_db,
        database=migrating_database,
    )
    execute_query(
        ctx,
        query,
        echo=True,
    )

    query = f"""
        SYSTEM SYNC DATABASE REPLICA {temp_db}
    """
    execute_query(
        ctx,
        query,
        echo=True,
        format_=None,
    )

    tables = create_tables_from_migrating_database(ctx, migrating_database, temp_db)

    metadata_non_repl_db = parse_database_from_metadata(migrating_database)
    metadata_temp_db = parse_database_from_metadata(temp_db)

    original_engine = metadata_non_repl_db.database_engine

    detach_dbs(ctx, dbs=[migrating_database, temp_db])

    metadata_non_repl_db.set_engine_from(metadata_temp_db)
    metadata_non_repl_db.update_metadata_file()

    update_zk_for_migrate(ctx, metadata_non_repl_db, tables)

    query = f"""
        ATTACH DATABASE {migrating_database}
    """
    execute_query(
        ctx,
        query,
        echo=True,
    )

    metadata_temp_db.database_engine = original_engine
    metadata_temp_db.update_metadata_file()

    remove_temp_db(ctx, metadata_temp_db)


def change_uuid_tables(ctx, tables, database_name):
    for row in tables:
        table_local_metadata_path = row["metadata_path"]
        if match_str_ch_version(get_version(ctx), "25.1"):
            table_local_metadata_path = CLICKHOUSE_PATH + "/" + row["metadata_path"]

        table_name = row["name"]
        database_name = row["database"]

        nodes = list_zk_nodes(ctx, f"/clickhouse/{database_name}/metadata")
        logging.info("Found nodes in metadata: {}", nodes)

        zk_metadata_path = f"/clickhouse/{database_name}/metadata/{table_name}"
        zk_table_metadata = get_zk_node(ctx, zk_metadata_path)

        logging.info("zk_table_metadata: \n{}\n", zk_table_metadata)

        # todo form zk?
        old_table_uuid = ""
        with open(table_local_metadata_path, "r", encoding="utf-8") as metadata_file:
            line = metadata_file.readline()
            if line.startswith("ATTACH TABLE") and metadata.UUID_PATTERN in line:
                old_table_uuid = metadata.parse_uuid(line)

        old_path = f"{CLICKHOUSE_PATH}/store/{old_table_uuid[:3]}/{old_table_uuid}"
        logging.info("old_path={}", old_path)

        assert os.path.exists(old_path)

        logging.info("old_table_uuid={} for table={}", old_table_uuid, table_name)

        with open(
            table_local_metadata_path, "w", encoding="utf-8"
        ) as local_metadata_file:
            local_metadata_file.write(zk_table_metadata + "\n")

        target_table_uuid = ""
        with open(table_local_metadata_path, "r", encoding="utf-8") as metadata_file:
            line = metadata_file.readline()
            if line.startswith("ATTACH TABLE") and metadata.UUID_PATTERN in line:
                # assert target_table_uuid is None
                target_table_uuid = metadata.parse_uuid(line)

        logging.info("New from zk  uuid={} for table={}", target_table_uuid, table_name)

        target = f"{CLICKHOUSE_PATH}/store/{target_table_uuid[:3]}"
        if not os.path.exists(target):
            # old = f"{CLICKHOUSE_PATH}/store/{old_table_uuid[:3]}"
            logging.info("need create path={}", target)
            # st = os.stat(old)
            # mode = os.stat.S_IMODE(st.st_mode)
            os.mkdir(target)
            os.chmod(target, 0o750)
            uid = pwd.getpwnam("clickhouse").pw_uid
            gid = grp.getgrnam("clickhouse").gr_gid

            os.chown(target, uid, gid)
        else:
            logging.info("path exists: {}", target)

        dst_path = (
            f"{CLICKHOUSE_PATH}/store/{target_table_uuid[:3]}/{target_table_uuid}"
        )
        logging.info("dst_path={}", dst_path)

        os.rename(old_path, dst_path)

        uid = pwd.getpwnam("clickhouse").pw_uid
        gid = grp.getgrnam("clickhouse").gr_gid
        os.chown(dst_path, uid, gid)

        # assert len(zk_table_metadata) == len(table_local_metadata_after)


def migrate_as_non_first_replica(ctx, database_name, temp_db):
    metadata_non_repl_db = parse_database_from_metadata(database_name)
    metadata_temp_db = parse_database_from_metadata(temp_db)

    original_engine = metadata_non_repl_db.database_engine

    tables = get_tables_local_metadata_path(ctx, database_name)

    detach_dbs(ctx, dbs=[database_name, temp_db])

    metadata_non_repl_db.set_engine_from(metadata_temp_db)
    metadata_non_repl_db.update_metadata_file()

    change_uuid_tables(ctx, tables, database_name)

    metadata_temp_db.database_engine = original_engine
    metadata_temp_db.update_metadata_file()

    remove_temp_db(ctx, metadata_temp_db)


@database_group.command("migrate")
@option("-d", "--database")
@pass_context
def migrate_engine_command(ctx, database):
    temp_db = f"temp_migrate_{database}"

    if is_first_replica_migrate(ctx, database):
        logging.info("First migrate")
        migrate_as_first_replica(ctx, database, temp_db)
    else:
        migrate_as_non_first_replica(ctx, database, temp_db)
