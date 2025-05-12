from click import ClickException, Context

from ch_tools.chadmin.cli import metadata
from ch_tools.chadmin.cli.database_metadata import (
    DatabaseEngine,
    DatabaseMetadata,
    parse_database_from_metadata,
)
from ch_tools.chadmin.internal.clickhouse_disks import CLICKHOUSE_PATH
from ch_tools.chadmin.internal.system import get_version, match_str_ch_version
from ch_tools.chadmin.internal.table import (
    change_table_uuid,
    detach_table,
    read_local_table_metadata,
)
from ch_tools.chadmin.internal.utils import execute_query, replace_macros
from ch_tools.chadmin.internal.zookeeper import (
    create_zk_nodes,
    delete_zk_node,
    get_zk_node,
    list_zk_nodes,
    update_zk_nodes,
)
from ch_tools.common import logging
from ch_tools.common.clickhouse.client.query_output_format import OutputFormat
from ch_tools.common.clickhouse.config import get_macros


def create_temp_db(ctx: Context, migrating_database: str, temp_db: str) -> None:
    query = """
        CREATE DATABASE {temp_db} ON CLUSTER '{{cluster}}' ENGINE = Replicated('/clickhouse/{database}', '{{shard}}', '{{replica}}')
    """.format(
        temp_db=temp_db,
        database=migrating_database,
    )

    response = execute_query(ctx, query, echo=True, format_="JSON")

    ex_text = response.get("exception")
    logging.info("create_temp_db: got ex={}", ex_text)
    if ex_text is not None:
        raise ClickException(ex_text)


def migrate_as_first_replica(ctx: Context, migrating_database: str) -> None:
    _create_database_metadata_nodes(ctx, migrating_database)

    _detach_dbs(ctx, dbs=[migrating_database])

    metadata_non_repl_db = parse_database_from_metadata(migrating_database)

    metadata_non_repl_db.database_engine = DatabaseEngine.REPLICATED
    metadata_non_repl_db.replica_path = (
        f"/clickhouse/{metadata_non_repl_db.database_name}"
    )
    metadata_non_repl_db.shard = "{shard}"
    metadata_non_repl_db.replica_name = "{replica}"

    metadata_non_repl_db.update_metadata_file()

    query = f"""
        ATTACH DATABASE {migrating_database}
    """

    # @todo discuss timeout and process error after attach
    execute_query(
        ctx,
        query,
        echo=True,
    )


def migrate_as_non_first_replica(ctx, database_name):
    metadata_non_repl_db = parse_database_from_metadata(database_name)
    tables_info = _get_tables_info_and_detach(ctx, database_name)

    # Unfortunately, it is not atomic.
    # The schema could be changed before detach databases.
    # However, it is sufficient for now, and we will stabilize
    # this logic after we stop creating temporary tables.
    for row in tables_info:
        table_name = row["name"]
        table_local_metadata_path = row["metadata_path"]

        if not is_table_schema_equal(
            ctx,
            database_name=database_name,
            table_name=table_name,
            table_local_metadata_path=table_local_metadata_path,
        ):
            if "Replicated" in row["engine"]:
                logging.warning(
                    "Replicated table engine {} can have different schema. Continue.",
                    row["engine"],
                )
                continue
            raise RuntimeError(
                f"Local table metadata for table {table_name} is different from zk metadata"
            )

    _detach_dbs(ctx, dbs=[database_name])

    metadata_non_repl_db.database_engine = DatabaseEngine.REPLICATED
    metadata_non_repl_db.replica_path = (
        f"/clickhouse/{metadata_non_repl_db.database_name}"
    )
    metadata_non_repl_db.shard = "{shard}"
    metadata_non_repl_db.replica_name = "{replica}"
    metadata_non_repl_db.update_metadata_file()

    was_changed = _change_tables_uuid(ctx, tables_info, database_name)

    if was_changed:
        logging.info(
            f"Table UUID was changed. Database {database_name} was detached. Need restart Clickhouse"
        )
    else:
        query = f"""
            ATTACH DATABASE {database_name}
        """

        # @todo discuss timeout and process error after attach
        execute_query(
            ctx,
            query,
            echo=True,
        )


# escapeForFileName
def _escape_hostname(s: str) -> str:
    def is_word_char_ascii(c):
        return c.isalnum() or c == "_"

    def hex_digit_uppercase(num):
        return format(num, "X")

    res = []
    pos = 0
    end = len(s)

    while pos != end:
        c = s[pos]

        if is_word_char_ascii(c):
            res.append(c)
        else:
            res.append("%")
            res.append(hex_digit_uppercase(ord(c) // 16))
            res.append(hex_digit_uppercase(ord(c) % 16))

        pos += 1

    return "".join(res)


# def _escape_hostname(hostname: str) -> str:
#     return quote(hostname, safe='')


def _get_host_id(ctx: Context, migrating_database: str, replica: str) -> str:
    # check with CH
    host_name = replica

    logging.info("host_name={}", host_name)

    query = f"""
        SELECT uuid FROM system.databases WHERE database='{migrating_database}'
    """
    rows = execute_query(ctx, query, echo=True, format_=OutputFormat.JSON)
    database_uuid = rows["data"][0]["uuid"]

    # port
    result = f"{_escape_hostname(host_name)}:9000:{database_uuid}"
    logging.info("_get_host_id={}", result)

    return result


def create_database_nodes(ctx: Context, migrating_database: str) -> None:
    # use tx
    # check exception
    # merge creating nodes to one request

    create_zk_nodes(
        ctx,
        [f"/clickhouse/{migrating_database}"],
        value="DatabaseReplicated",
    )

    # logic with cnt
    create_zk_nodes(
        ctx,
        [f"/clickhouse/{migrating_database}/counter"],
    )

    create_zk_nodes(
        ctx,
        [f"/clickhouse/{migrating_database}/counter/cnt-"],
    )

    delete_zk_node(
        ctx,
        f"/clickhouse/{migrating_database}/counter/cnt-",
    )

    data_first_replica = migrating_database
    create_zk_nodes(
        ctx,
        [f"/clickhouse/{migrating_database}/first_replica_database_name"],
        value=data_first_replica,
    )

    # issue: /clickhouse/non_repl_db/log/query-0000000003
    # /clickhouse/temp_db_1/log/query-0000000001
    # When this node will be flushed?
    create_zk_nodes(
        ctx,
        [f"/clickhouse/{migrating_database}/log"],
    )

    # data_log_queue="version: 1\nquery: \nhosts: []\ninitiator:"

    # create_zk_nodes(
    #     ctx,
    #     [f"/clickhouse/{migrating_database}/log/query-0000000001"],
    #     value=data_log_queue,
    # )

    # create_zk_nodes(
    #     ctx,
    #     [f"/clickhouse/{migrating_database}/log/query-0000000001/active"],
    # )

    # shard = replace_macros("{shard}", get_macros(ctx))
    # replica = replace_macros("{replica}", get_macros(ctx))

    # create_zk_nodes(
    #     ctx,
    #     [f"/clickhouse/{migrating_database}/log/query-0000000001/committed"],
    #     value=f"{shard}|{replica}"
    # )

    # create_zk_nodes(
    #     ctx,
    #     [f"/clickhouse/{migrating_database}/log/query-0000000001/finished"],
    # )

    # create_zk_nodes(
    #     ctx,
    #     [f"/clickhouse/{migrating_database}/log/query-0000000001/finished/{shard}|{replica}"],
    #     value="0",
    # )

    # create_zk_nodes(
    #     ctx,
    #     [f"/clickhouse/{migrating_database}/log/query-0000000001/synced"],
    # )

    data_logs_to_keep = "1000"
    create_zk_nodes(
        ctx,
        [f"/clickhouse/{migrating_database}/logs_to_keep"],
        value=data_logs_to_keep,
    )

    max_log_ptr = "1"
    create_zk_nodes(
        ctx,
        [f"/clickhouse/{migrating_database}/max_log_ptr"],
        value=max_log_ptr,
    )

    create_zk_nodes(
        ctx,
        [f"/clickhouse/{migrating_database}/metadata"],
    )

    create_zk_nodes(
        ctx,
        [f"/clickhouse/{migrating_database}/replicas"],
    )


def create_database_replica(ctx: Context, migrating_database: str) -> None:
    shard = replace_macros("{shard}", get_macros(ctx))
    replica = replace_macros("{replica}", get_macros(ctx))

    replica_node = f"/clickhouse/{migrating_database}/replicas/{shard}|{replica}"
    logging.info("create_database_replica: {}", replica_node)
    create_zk_nodes(
        ctx, [replica_node], value=_get_host_id(ctx, migrating_database, replica)
    )

    query = """
        SELECT serverUUID() as id
    """
    rows = execute_query(ctx, query, echo=True, format_=OutputFormat.JSON)
    server_uuid = rows["data"][0]["id"]
    logging.info("rows={}, server_uuid={}", rows, server_uuid)

    create_zk_nodes(
        ctx,
        [replica_node + "/active"],
        value=server_uuid,
    )

    create_zk_nodes(
        ctx,
        [replica_node + "/digest"],
        value="0",
    )

    create_zk_nodes(
        ctx,
        [replica_node + "/log_ptr"],
        value="0",
    )

    create_zk_nodes(ctx, [replica_node + "/max_log_ptr_at_creation"], value="1")

    # specific logic
    create_zk_nodes(
        ctx,
        [replica_node + "/replica_group"],
    )


def _create_database_metadata_nodes(ctx: Context, migrating_database: str) -> None:
    query = f"""
        SELECT name, create_table_query, metadata_path FROM system.tables WHERE database='{migrating_database}'
    """
    rows = execute_query(ctx, query, echo=True, format_=OutputFormat.JSON)

    for row in rows["data"]:
        table_name = row["name"]
        metadata_path = row["metadata_path"]

        if match_str_ch_version(get_version(ctx), "25.1"):
            metadata_path = CLICKHOUSE_PATH + "/" + metadata_path

        with open(metadata_path, "r", encoding="utf-8") as metadata_file:
            local_table_metadata = metadata_file.read()

            # use tx ?
            # check exception
            create_zk_nodes(
                ctx,
                [f"/clickhouse/{migrating_database}/metadata/{table_name}"],
                value=local_table_metadata,
            )

            logging.info(
                "add from file with metadata to tables: {}",
                local_table_metadata,
            )


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

    # _update_zk_tables_metadata(ctx, zk_db_path, mapping_table_to_metadata)

    for replica_path in list_zk_nodes(ctx, zk_db_path + "/replicas"):
        replica_data = get_zk_node(ctx, replica_path)

        prefix = replica_data.split(":")
        new_data = f"{prefix[0]}:{prefix[1]}:{metadata_non_repl_db.database_uuid}"

        logging.info(
            "Update replica: {}, from {} to {}", replica_path, replica_data, new_data
        )
        update_zk_nodes(ctx, [replica_path], new_data)

        logging.info("Updating was finished")


# useles?
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
        SELECT database, name, uuid, create_table_query, metadata_path, engine FROM system.tables WHERE database='{database_name}'
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

        # is it neccassary?
        detach_table(
            ctx, database_name=database_name, table_name=table_name, permanently=False
        )

    logging.info("Database {} contains tables: {}", database_name, tables)
    return tables


def is_table_schema_equal(
    ctx: Context, database_name: str, table_name: str, table_local_metadata_path: str
) -> bool:
    zk_metadata_path = f"/clickhouse/{database_name}/metadata/{table_name}"
    zk_table_metadata = get_zk_node(ctx, zk_metadata_path)

    local_table_metadata = read_local_table_metadata(ctx, table_local_metadata_path)

    zk_table_metadata = zk_table_metadata.rstrip()
    local_table_metadata = local_table_metadata.rstrip()

    # Do not need use regex because we have stable table metadata format
    start_pos_uuid = 21
    finish_pos_uuid = 57

    local_table_metadata = (
        local_table_metadata[:start_pos_uuid] + local_table_metadata[finish_pos_uuid:]
    )
    zk_table_metadata = (
        zk_table_metadata[:start_pos_uuid] + zk_table_metadata[finish_pos_uuid:]
    )

    logging.info(
        "Compare metadata: local={}, zk={}", local_table_metadata, zk_table_metadata
    )

    return local_table_metadata == zk_table_metadata


def _change_tables_uuid(ctx: Context, tables: dict, database_name: str) -> bool:
    was_changed = False
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

        was_changed = True

        change_table_uuid(
            ctx,
            database=database_name,
            table=table_name,
            new_uuid=zk_table_uuid,
            old_table_uuid=old_table_uuid,
            table_local_metadata_path=table_local_metadata_path,
            attached=False,
        )

    return was_changed


def is_database_exists(ctx: Context, database_name: str) -> bool:
    query = f"""
        SELECT 1 FROM system.databases WHERE database='{database_name}'
    """
    rows = execute_query(ctx, query, echo=True, format_=OutputFormat.JSON)

    return 1 == len(rows["data"])
