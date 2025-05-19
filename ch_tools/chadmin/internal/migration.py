from click import ClickException, Context

from ch_tools.chadmin.cli import metadata
from ch_tools.chadmin.cli.database_metadata import (
    DatabaseMetadata,
    parse_database_from_metadata,
)
from ch_tools.chadmin.internal.clickhouse_disks import CLICKHOUSE_PATH
from ch_tools.chadmin.internal.system import get_version, match_str_ch_version
from ch_tools.chadmin.internal.table import change_table_uuid, detach_table
from ch_tools.chadmin.internal.table_metadata import remove_replicated_params
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.chadmin.internal.zookeeper import (
    get_zk_node,
    list_zk_nodes,
    update_zk_nodes,
)
from ch_tools.common import logging
from ch_tools.common.clickhouse.client.query_output_format import OutputFormat


def create_temp_db(ctx: Context, migrating_database: str, temp_db: str) -> None:
    query = f"""
        CREATE DATABASE {temp_db} ON CLUSTER '{{cluster}}' ENGINE = Replicated('/clickhouse/{migrating_database}', '{{shard}}', '{{replica}}')
    """

    response = execute_query(ctx, query, echo=True, format_="JSON")

    ex_text = response.get("exception")
    logging.info("create_temp_db: got ex={}", ex_text)
    if ex_text is not None:
        raise ClickException(ex_text)


def migrate_as_first_replica(
    ctx: Context, migrating_database: str, temp_db: str
) -> None:

    mapping_table_to_metadata = _create_tables_from_migrating_database(
        ctx, migrating_database, temp_db
    )

    metadata_non_repl_db = parse_database_from_metadata(migrating_database)
    metadata_temp_db = parse_database_from_metadata(temp_db)

    original_engine = metadata_non_repl_db.database_engine

    _detach_dbs(ctx, dbs=[migrating_database, temp_db])

    metadata_non_repl_db.set_engine_from(metadata_temp_db)
    metadata_non_repl_db.update_metadata_file()

    _update_zk_for_migrate(ctx, metadata_non_repl_db, mapping_table_to_metadata)

    query = f"""
        ATTACH DATABASE {migrating_database}
    """

    # @todo discuss timeout and process error after attach
    execute_query(
        ctx,
        query,
        echo=True,
    )

    metadata_temp_db.database_engine = original_engine
    metadata_temp_db.update_metadata_file()

    _remove_temp_db(ctx, metadata_temp_db)


def migrate_as_non_first_replica(ctx, database_name, temp_db):
    metadata_non_repl_db = parse_database_from_metadata(database_name)
    metadata_temp_db = parse_database_from_metadata(temp_db)
    original_engine = metadata_non_repl_db.database_engine
    tables_info = _get_tables_info_and_detach(ctx, database_name)

    _detach_dbs(ctx, dbs=[database_name, temp_db])

    metadata_non_repl_db.set_engine_from(metadata_temp_db)
    metadata_non_repl_db.update_metadata_file()

    _change_tables_uuid(ctx, tables_info, database_name)

    metadata_temp_db.database_engine = original_engine
    metadata_temp_db.update_metadata_file()

    _remove_temp_db(ctx, metadata_temp_db)


def _create_tables_from_migrating_database(
    ctx: Context, migrating_database: str, temp_db: str
) -> dict:
    query = f"""
        SELECT name, create_table_query, metadata_path FROM system.tables WHERE database='{migrating_database}'
    """
    rows = execute_query(ctx, query, echo=True, format_=OutputFormat.JSON)

    mapping_table_to_metadata = {}

    for row in rows["data"]:
        table_name = row["name"]
        metadata_path = row["metadata_path"]
        create_table_query = row["create_table_query"]

        logging.info(
            "table_name={}, metadata_path={}, create_table_query=[{}]",
            table_name,
            metadata_path,
            create_table_query,
        )

        create_table_query = create_table_query.replace(migrating_database, temp_db, 1)
        logging.info(
            "after replacing database create_table_query=[{}]", create_table_query
        )

        # If we want to create ReplicatedMergeTree table in Replicated database
        # we can't use params (zookeeper path and replica name)
        create_table_query = remove_replicated_params(create_table_query)

        logging.info(
            "after replacing replicated params create_table_query=[{}]",
            create_table_query,
        )

        execute_query(
            ctx,
            create_table_query,
            echo=True,
        )

        if match_str_ch_version(get_version(ctx), "25.1"):
            metadata_path = CLICKHOUSE_PATH + "/" + metadata_path

        with open(metadata_path, "r", encoding="utf-8") as metadata_file:
            mapping_table_to_metadata[table_name] = metadata_file.read()
            logging.info(
                "add from file with metadata to tables: {}",
                mapping_table_to_metadata[table_name],
            )

    logging.info("total mapping_table_to_metadata: {}", mapping_table_to_metadata)
    return mapping_table_to_metadata


def _detach_dbs(ctx: Context, dbs: list) -> None:
    for db in dbs:
        query = f"""
            DETACH DATABASE {db}
        """
        execute_query(
            ctx,
            query,
            echo=True,
        )


def _update_zk_for_migrate(
    ctx: Context,
    metadata_non_repl_db: DatabaseMetadata,
    mapping_table_to_metadata: dict,
) -> None:
    """
    Changing the database information in Zookeeper's nodes from the temporary database to the original database.
    After this update, the original database will be able to use these nodes.
    We will remove the temporary database later.
    """

    zk_db_path = f"/clickhouse/{metadata_non_repl_db.database_name}"
    first_replica_database_name = zk_db_path + "/first_replica_database_name"
    node_data = get_zk_node(ctx, first_replica_database_name)

    logging.info(
        "first_replica_database_name={} contains: {}",
        first_replica_database_name,
        node_data,
    )

    if node_data == metadata_non_repl_db.database_name:
        logging.info(
            "first_replica_database_name was migrated early. Skip updating zookeeper."
        )
        return

    logging.info(
        "update first_replica_database_name, path {}", first_replica_database_name
    )

    # I assume that we need to use a transaction here
    # because the second replica could perform the migration as well.
    # Therefore, we do not have guarantees that nodes with metadata
    # will be ready to work with the second node.
    # However, if we work sequentially, it would be reliable.
    update_zk_nodes(
        ctx, [first_replica_database_name], metadata_non_repl_db.database_name
    )

    _update_zk_tables_metadata(ctx, zk_db_path, mapping_table_to_metadata)

    for replica_path in list_zk_nodes(ctx, zk_db_path + "/replicas"):
        replica_data = get_zk_node(ctx, replica_path)

        prefix = replica_data.split(":")
        new_data = f"{prefix[0]}:{prefix[1]}:{metadata_non_repl_db.database_uuid}"

        logging.info(
            "Update replica: {}, from {} to {}", replica_path, replica_data, new_data
        )
        update_zk_nodes(ctx, [replica_path], new_data)

        logging.info("Updating was finished")


def _update_zk_tables_metadata(
    ctx: Context, zk_db_path: str, mapping_table_to_metadata: dict
) -> None:
    for metadata_path in list_zk_nodes(ctx, zk_db_path + "/metadata"):
        logging.info("update metadata path={}", metadata_path)

        data = get_zk_node(ctx, metadata_path)

        logging.info(
            "Found node for table. path={}, contains data={}", metadata_path, data
        )
        table_name = metadata_path.split("/")[-1]
        target_metadata = mapping_table_to_metadata[table_name]

        logging.info(
            "New metadata for node from mapping table:\n{}\n===", target_metadata
        )

        update_zk_nodes(ctx, [metadata_path], target_metadata)


def _remove_temp_db(ctx: Context, metadata_temp_db: DatabaseMetadata) -> None:
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


def _get_tables_info_and_detach(ctx: Context, database_name: str) -> dict:
    query = f"""
        SELECT database, name, uuid, create_table_query, metadata_path FROM system.tables WHERE database='{database_name}'
    """
    rows = execute_query(ctx, query, echo=True, format_=OutputFormat.JSON)

    tables = rows["data"]

    for row in rows["data"]:
        logging.info("got row={}", row)
        table_name = row["name"]
        metadata_path = row["metadata_path"]
        create_table_query = row["create_table_query"]

        logging.info(
            "table_name={}, metadata_path={}, create_table_query={}",
            table_name,
            metadata_path,
            create_table_query,
        )

        detach_table(
            ctx, database_name=database_name, table_name=table_name, permanently=False
        )

    logging.info("Database {} contains tables: {}", database_name, tables)
    return tables


def _change_tables_uuid(ctx: Context, tables: dict, database_name: str) -> None:
    for row in tables:
        table_local_metadata_path = row["metadata_path"]

        table_name = row["name"]
        database_name = row["database"]
        old_table_uuid = row["uuid"]

        zk_metadata_path = f"/clickhouse/{database_name}/metadata/{table_name}"
        zk_table_metadata = get_zk_node(ctx, zk_metadata_path)

        zk_table_uuid = metadata.parse_uuid(zk_table_metadata)

        logging.info(
            "Table {} has old_table_uuid={}, zk_table_uuid={}",
            table_name,
            old_table_uuid,
            zk_table_uuid,
        )

        if zk_table_uuid == old_table_uuid:
            logging.info("Equal uuid. Don't need to change uuid.")
            continue

        change_table_uuid(
            ctx,
            database=database_name,
            table=table_name,
            new_uuid=zk_table_uuid,
            old_table_uuid=old_table_uuid,
            table_local_metadata_path=table_local_metadata_path,
            attached=False,
        )


def is_database_exists(ctx: Context, database_name: str) -> bool:
    query = f"""
        SELECT 1 FROM system.databases WHERE database='{database_name}'
    """
    rows = execute_query(ctx, query, echo=True, format_=OutputFormat.JSON)

    return 1 == len(rows["data"])
