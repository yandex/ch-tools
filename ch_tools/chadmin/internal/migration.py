from typing import List, Optional, Tuple

from click import ClickException, Context
from kazoo.client import KazooClient, TransactionRequest
from kazoo.exceptions import NodeExistsError

from ch_tools.chadmin.cli import metadata
from ch_tools.chadmin.cli.database_metadata import (
    parse_database_from_metadata,
    remove_uuid_from_metadata,
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
    delete_zk_node,
    escape_for_zookeeper,
    format_path,
    get_zk_node,
    zk_client,
)
from ch_tools.common import logging
from ch_tools.common.clickhouse.client.query_output_format import OutputFormat
from ch_tools.common.clickhouse.config import get_clickhouse_config, get_macros
from ch_tools.common.clickhouse.config.clickhouse import ClickhousePort


def create_temp_db(ctx: Context, migrating_database: str, temp_db: str) -> None:
    query = f"""
        CREATE DATABASE {temp_db} ON CLUSTER '{{cluster}}' ENGINE = Replicated('/clickhouse/{migrating_database}', '{{shard}}', '{{replica}}')
    """

    response = execute_query(ctx, query, echo=True, format_="JSON")

    ex_text = response.get("exception")
    logging.info("create_temp_db: got ex={}", ex_text)
    if ex_text is not None:
        raise ClickException(ex_text)


def _update_local_metadata_first_replica(ctx: Context, migrating_database: str) -> None:
    _detach_dbs(ctx, dbs=[migrating_database])

    try:
        metadata_non_repl_db = parse_database_from_metadata(migrating_database)
        metadata_non_repl_db.set_replicated()
    except Exception as ex:
        logging.error("Failed _update_local_metadata_first_replica with ex={}", ex)
        _attach_dbs(ctx, dbs=[migrating_database])
        raise ex

    _attach_dbs(ctx, dbs=[migrating_database])


def _check_tables_consistent(
    ctx: Context, database_name: str, tables_info: dict
) -> None:
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

            logging.error(
                "Table {} with engine {} has different schema.",
                table_name,
                row["engine"],
            )
            _attach_dbs(ctx, dbs=[database_name])

            raise RuntimeError(
                f"Local table metadata for table {table_name} is different from zk metadata"
            )


def _generate_counter(ctx: Context, zk: KazooClient, db_zk_path: str) -> str:
    path_counter = zk.create(
        format_path(ctx, f"{db_zk_path}/counter/cnt-"),
        sequence=True,
        ephemeral=True,
    )

    counter = path_counter[path_counter.rfind("-") + 1 :]
    assert len(counter) > 0

    logging.info(
        "path_counter={}, after parse counter={}",
        path_counter,
        counter,
    )

    return counter


def _check_result_txn(results: List) -> None:
    for result in results:
        if isinstance(result, NodeExistsError):
            logging.info("result contains NodeExistsError.")
            raise NodeExistsError()
        if isinstance(result, Exception):
            logging.error("result contains ex={}, type=P{}.", result, type(result))
            raise result
        logging.info("check_result_txn: result={}, continue.", result)


def get_shard_and_replica_from_macros(ctx: Context) -> Tuple[str, str]:
    macros = get_macros(ctx)

    missing = [macro for macro in ["shard", "replica"] if macro not in macros]
    if missing:
        raise RuntimeError(f"Failed replace marcos. {missing}")

    shard = replace_macros("{shard}", get_macros(ctx))
    replica = replace_macros("{replica}", get_macros(ctx))

    return shard, replica


def migrate_as_first_replica(ctx: Context, migrating_database: str) -> None:
    logging.info("call migrate_as_first_replica")

    restore_replica(ctx, migrating_database, first_replica=True)

    # There is an issue when at first time _update_local_metadata_first_replica was failed.
    # Then next time we would have a failed txn.
    # However, I assume that database has correct metadata.
    _update_local_metadata_first_replica(ctx, migrating_database)


def restore_replica(
    ctx: Context,
    database_name: str,
    first_replica: bool,
    db_replica_path: Optional[str] = None,
) -> None:
    logging.info("call restore_replica: as first {}", first_replica)

    prefix_db_zk_path = db_replica_path or _default_db_zk_path(database_name)

    with zk_client(ctx) as zk:
        counter = _generate_counter(ctx, zk, prefix_db_zk_path)
        txn = zk.transaction()

        if first_replica:
            _create_first_replica_database_name(
                ctx,
                txn,
                prefix_db_zk_path=prefix_db_zk_path,
                migrating_database=database_name,
            )
        _create_query_node(ctx, txn, prefix_db_zk_path, counter)
        _create_database_replica(ctx, txn, database_name, prefix_db_zk_path)

        if first_replica:
            _create_database_metadata_nodes(
                ctx,
                txn,
                database_name,
                prefix_db_zk_path,
            )

        result = txn.commit()
        logging.info("Txn was committed. Result {}", result)

        _check_result_txn(result)


def migrate_as_non_first_replica(ctx: Context, migrating_database: str) -> None:
    logging.info("call migrate_as_non_first_replica")

    prefix_db_zk_path = _default_db_zk_path(migrating_database)

    with zk_client(ctx) as zk:
        counter = _generate_counter(ctx, zk, prefix_db_zk_path)

        txn = zk.transaction()

        _create_query_node(ctx, txn, prefix_db_zk_path, counter)
        _create_database_replica(
            ctx,
            txn,
            migrating_database,
            prefix_db_zk_path=prefix_db_zk_path,
        )

        metadata_non_repl_db = parse_database_from_metadata(migrating_database)
        tables_info = _get_tables_info_and_detach(ctx, migrating_database)

        _detach_dbs(ctx, dbs=[migrating_database])

        _check_tables_consistent(ctx, migrating_database, tables_info)

        result = txn.commit()
        logging.info("Txn was committed. Result {}", result)

        _check_result_txn(result)

    metadata_non_repl_db.set_replicated()
    was_changed = _change_tables_uuid(ctx, tables_info, migrating_database)

    if was_changed:
        logging.info(
            f"Table UUID was changed. Database {migrating_database} was detached. Need restart Clickhouse."
        )
    else:
        _attach_dbs(ctx, dbs=[migrating_database])


def _get_host_id(ctx: Context, migrating_database: str, replica: str) -> str:
    host_name = replica

    logging.info("host_name={}", host_name)

    query = f"""
        SELECT uuid FROM system.databases WHERE database='{migrating_database}'
    """
    rows = execute_query(ctx, query, echo=True, format_=OutputFormat.JSON)
    database_uuid = rows["data"][0]["uuid"]

    ch_server_config = get_clickhouse_config(ctx)
    tcp_port = ch_server_config.ports[ClickhousePort.TCP]

    result = f"{escape_for_zookeeper(host_name)}:{tcp_port}:{database_uuid}"
    logging.info("_get_host_id={}", result)

    return result


def _default_db_zk_path(database_name: str) -> str:
    return f"/clickhouse/{database_name}"


def create_database_nodes(
    ctx: Context,
    database_name: str,
    db_replica_path: Optional[str] = None,
) -> None:
    """
    Create common nodes for Replicated database.
    https://github.com/ClickHouse/ClickHouse/blob/master/src/Databases/DatabaseReplicated.cpp#L581
    """

    with zk_client(ctx) as zk:
        if db_replica_path:
            prefix_db_zk_path = db_replica_path
        else:
            prefix_db_zk_path = _default_db_zk_path(database_name)
            if not zk.exists(format_path(ctx, "/clickhouse")):
                zk.create(format_path(ctx, "/clickhouse"), makepath=True)

        txn = zk.transaction()

        txn.create(
            path=format_path(ctx, prefix_db_zk_path),
            value="DatabaseReplicated".encode(),
        )

        txn.create(path=format_path(ctx, f"{prefix_db_zk_path}/log"))
        txn.create(path=format_path(ctx, f"{prefix_db_zk_path}/replicas"))

        txn.create(path=format_path(ctx, f"{prefix_db_zk_path}/counter"))

        txn.create(path=format_path(ctx, f"{prefix_db_zk_path}/counter/cnt-"))
        txn.delete(path=format_path(ctx, f"{prefix_db_zk_path}/counter/cnt-"))

        txn.create(path=format_path(ctx, f"{prefix_db_zk_path}/metadata"))

        max_log_ptr = "1"
        txn.create(
            path=format_path(ctx, f"{prefix_db_zk_path}/max_log_ptr"),
            value=max_log_ptr.encode(),
        )

        data_logs_to_keep = "1000"
        txn.create(
            path=format_path(ctx, f"{prefix_db_zk_path}/logs_to_keep"),
            value=data_logs_to_keep.encode(),
        )

        result = txn.commit()
        logging.debug("Txn was committed. Result {}", result)

        _check_result_txn(result)

        logging.debug("result does not contain NodeExistsError.")


def _create_first_replica_database_name(
    ctx: Context,
    txn: TransactionRequest,
    prefix_db_zk_path: str,
    migrating_database: str,
) -> None:
    logging.debug("call create_first_replica_database_name.")

    txn.create(
        path=format_path(ctx, f"{prefix_db_zk_path}/first_replica_database_name"),
        value=migrating_database.encode(),
    )


def _create_query_node(
    ctx: Context, txn: TransactionRequest, prefix_db_zk_path: str, counter: str
) -> None:
    """
    Create queue nodes for Replicated database.
    https://github.com/ClickHouse/ClickHouse/blob/master/src/Databases/DatabaseReplicatedWorker.cpp#L254
    """
    logging.debug("call create_query_node with counter={}.", counter)

    data_log_queue = """version: 1
query: 
hosts: []
initiator: 
"""  # noqa: W291

    txn.create(
        path=format_path(ctx, f"{prefix_db_zk_path}/log/query-{counter}"),
        value=data_log_queue.encode(),
    )

    shard, replica = get_shard_and_replica_from_macros(ctx)
    txn.create(
        path=format_path(ctx, f"{prefix_db_zk_path}/log/query-{counter}/committed"),
        value=f"{shard}|{replica}".encode(),
    )

    txn.create(
        path=format_path(ctx, f"{prefix_db_zk_path}/log/query-{counter}/active"),
    )

    txn.create(
        path=format_path(ctx, f"{prefix_db_zk_path}/log/query-{counter}/finished"),
    )

    txn.create(
        path=format_path(ctx, f"{prefix_db_zk_path}/log/query-{counter}/synced"),
    )


def _create_database_replica(
    ctx: Context,
    txn: TransactionRequest,
    migrating_database: str,
    prefix_db_zk_path: str,
) -> None:
    """
    Create replica nodes for Replicated database.
    https://github.com/ClickHouse/ClickHouse/blob/master/src/Databases/DatabaseReplicated.cpp#L714
    """
    logging.info("call create_database_replica")

    shard, replica = get_shard_and_replica_from_macros(ctx)

    txn.create(
        path=format_path(
            ctx,
            f"{prefix_db_zk_path}/log/query-0000000001/finished/{shard}|{replica}",
        ),
        value="0".encode(),
    )

    replica_node = f"{prefix_db_zk_path}/replicas/{shard}|{replica}"

    txn.create(
        path=format_path(ctx, replica_node),
        value=_get_host_id(ctx, migrating_database, replica).encode(),
    )

    query = """
        SELECT serverUUID() as id
    """
    rows = execute_query(ctx, query, echo=True, format_=OutputFormat.JSON)
    server_uuid = rows["data"][0]["id"]
    logging.info("rows={}, server_uuid={}", rows, server_uuid)

    txn.create(
        path=format_path(ctx, replica_node + "/active"),
        value=server_uuid.encode(),
    )

    txn.create(
        path=format_path(ctx, replica_node + "/digest"),
        value="0".encode(),
    )

    txn.create(
        path=format_path(ctx, replica_node + "/log_ptr"),
        value="0".encode(),
    )

    txn.create(
        path=format_path(ctx, replica_node + "/max_log_ptr_at_creation"),
        value="1".encode(),
    )


def _create_database_metadata_nodes(
    ctx: Context,
    txn: TransactionRequest,
    migrating_database: str,
    prefix_db_zk_path: str,
) -> None:
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

            txn.create(
                path=format_path(ctx, f"{prefix_db_zk_path}/metadata/{table_name}"),
                value=local_table_metadata.encode(),
            )

            logging.info(
                "add table metadata to txn: {}",
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


def _attach_dbs(ctx: Context, dbs: list) -> None:
    for db in dbs:
        query = f"""
            ATTACH DATABASE {db}
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

    local_table_metadata = remove_uuid_from_metadata(local_table_metadata)
    zk_table_metadata = remove_uuid_from_metadata(zk_table_metadata)

    logging.info(
        "Compare metadata: local={}, zk={}", local_table_metadata, zk_table_metadata
    )

    return local_table_metadata == zk_table_metadata


def _change_tables_uuid(ctx: Context, tables_info: dict, database_name: str) -> bool:
    was_changed = False
    for row in tables_info:
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
            engine=row["engine"],
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


def migrate_database_to_replicated(ctx: Context, database: str) -> None:
    first_replica = True
    try:
        create_database_nodes(ctx, database)
    except NodeExistsError as ex:
        logging.debug(
            "create_database_nodes failed with NodeExistsError. {}, type={}. Migrate as second replica",
            ex,
            type(ex),
        )

        first_replica = False
    except Exception as ex:
        logging.error("create_database_nodes failed with ex={}", type(ex))
        raise ex

    if first_replica:
        migrate_as_first_replica(ctx, database)
    else:
        migrate_as_non_first_replica(ctx, database)


def migrate_database_to_atomic(
    ctx: Context, database: str, clean_zookeeper: bool
) -> None:
    metadata_repl_db = parse_database_from_metadata(database)
    _detach_dbs(ctx, dbs=[database])
    try:
        replica_path = metadata_repl_db.replica_path
        metadata_repl_db.set_atomic()

        if clean_zookeeper:
            logging.debug(
                "Set clean_zookeeper - delete zookeeper nodes {}",
                replica_path,
            )
            delete_zk_node(ctx, replica_path)

    except Exception as ex:
        logging.error("Failed set atomic in metadata: {}", ex)
        _attach_dbs(ctx, dbs=[database])
        raise ex

    logging.info("Metadata {} was updated to Atomic", database)
    _attach_dbs(ctx, dbs=[database])
