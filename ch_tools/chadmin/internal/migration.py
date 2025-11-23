from operator import itemgetter
from typing import Any, List, Literal, Optional, Tuple

from click import ClickException, Context
from kazoo.client import KazooClient, TransactionRequest
from kazoo.exceptions import NodeExistsError, NoNodeError

from ch_tools.chadmin.cli import metadata
from ch_tools.chadmin.cli.database_metadata import (
    DatabaseEngine,
    parse_database_metadata,
    remove_uuid_from_metadata,
)
from ch_tools.chadmin.internal.clickhouse_disks import CLICKHOUSE_PATH
from ch_tools.chadmin.internal.system import match_ch_version
from ch_tools.chadmin.internal.table import (
    change_table_uuid,
    detach_table,
    list_tables,
    read_local_table_metadata,
)
from ch_tools.chadmin.internal.table_info import TableInfo
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


class AttacherContext:
    ctx: Context
    database: str

    def __init__(self, ctx: Context, database: str):
        self.ctx = ctx
        self.database = database

    def __enter__(self) -> None:
        _detach_dbs(self.ctx, dbs=[self.database])

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_value: Optional[Exception],
        traceback: Optional[Any],
    ) -> Literal[False]:
        _attach_dbs(self.ctx, dbs=[self.database])
        if exc_type is not None:
            logging.error(
                f"An exception occurred in AttacherContext: {exc_type.__name__}: {exc_value}"
            )
        return False


class ZKTransactionBuilder:
    ctx: Context
    txn: TransactionRequest

    path_to_nodes: List[str]

    def __init__(self, ctx: Context, zk: KazooClient) -> None:
        self.ctx = ctx
        self.txn = zk.transaction()
        self.path_to_nodes = []

    def create_node(self, path: str, value: str = "") -> None:
        self.path_to_nodes.append(path)
        self.txn.create(path=format_path(self.ctx, path), value=value.encode())

    def delete_node(self, path: str) -> None:
        self.path_to_nodes.append(path)
        self.txn.delete(path=format_path(self.ctx, path))

    def commit(self) -> None:
        result = self.txn.commit()

        for status in zip(self.path_to_nodes, result):
            logging.info(f"{status}")

        self.path_to_nodes = []
        ZKTransactionBuilder._check_result_txn(result)

    @staticmethod
    def _check_result_txn(results: List) -> None:
        for result in results:
            if isinstance(result, NodeExistsError):
                logging.debug("result contains NodeExistsError.")
                raise NodeExistsError()
            if isinstance(result, Exception):
                logging.error("result contains ex={}, type={}.", result, type(result))
                raise result
            logging.debug("check_result_txn: result={}, continue.", result)


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
    with AttacherContext(ctx, migrating_database) as _:
        try:
            metadata_non_repl_db = parse_database_metadata(migrating_database)
            metadata_non_repl_db.set_replicated()
        except Exception as ex:
            logging.error("Failed _update_local_metadata_first_replica with ex={}", ex)
            raise ex


def _get_zk_tables_metadata(ctx: Context, database_name: str) -> dict[str, str]:
    """
    Get table names and their metadata for Replicated database from ZooKeeper.
    """
    zk_tables_metadata = {}

    with zk_client(ctx) as zk:
        zk_metadata_path = f"/clickhouse/{database_name}/metadata"
        children: list[str] = zk.get_children(zk_metadata_path)
        for table_name in children:
            try:
                table_metadata_path = f"{zk_metadata_path}/{table_name}"
                zk_tables_metadata[table_name] = (
                    zk.get(table_metadata_path)[0].decode().strip()
                )
            except NoNodeError:
                logging.warning(f"Table {table_name} metadata was removed concurrently")

    return zk_tables_metadata


def _check_tables_consistent(
    ctx: Context, database_name: str, local_tables: list[TableInfo]
) -> None:
    zk_tables = _get_zk_tables_metadata(ctx, database_name)
    missing_in_zk = []
    schema_mismatches = []

    for table in local_tables:
        if table["name"] not in zk_tables:
            missing_in_zk.append(table["name"])
            continue

        if not _compare_table_schemas(
            table["name"],
            read_local_table_metadata(ctx, table["metadata_path"]),
            zk_tables[table["name"]],
        ):
            schema_mismatches.append(table["name"])

    if missing_in_zk or schema_mismatches:
        error_msg = f"Database '{database_name}' tables are inconsistent."
        if missing_in_zk:
            error_msg += f"\nMissing in ZK: {missing_in_zk}."
        if schema_mismatches:
            error_msg += f"\nSchema mismatches: {schema_mismatches}."

        logging.error(error_msg)
        logging.error("Local tables: {}", sorted(local_tables, key=itemgetter("name")))
        logging.error("ZooKeeper tables: {}", sorted(zk_tables))

        raise RuntimeError(error_msg)


def _compare_table_schemas(
    table_name: str,
    local_table_metadata: str,
    zk_table_metadata: str,
) -> bool:
    """
    Compare local table schema with ZooKeeper metadata except for UUID.
    """
    zk_table_metadata = zk_table_metadata.rstrip()
    local_table_metadata = local_table_metadata.rstrip()
    logging.debug(
        f"Table {table_name} has local metadata={local_table_metadata} and zookeeper metadata={zk_table_metadata}"
    )

    local_table_metadata = remove_uuid_from_metadata(local_table_metadata)
    zk_table_metadata = remove_uuid_from_metadata(zk_table_metadata)

    if local_table_metadata != zk_table_metadata:
        logging.warning(
            "Table {}: local metadata differs from zookeeper metadata", table_name
        )
        return False

    logging.info(
        "Table {}: local metadata is the same as zookeeper metadata", table_name
    )
    return True


def _generate_counter(ctx: Context, zk: KazooClient, db_zk_path: str) -> str:
    path_counter = zk.create(
        format_path(ctx, f"{db_zk_path}/counter/cnt-"),
        sequence=True,
        ephemeral=True,
    )

    counter = path_counter[path_counter.rfind("-") + 1 :]
    assert len(counter) > 0

    logging.debug(
        "path_counter={}, after parse counter={}",
        path_counter,
        counter,
    )

    return counter


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
        builder = ZKTransactionBuilder(ctx, zk)

        if first_replica:
            _create_first_replica_database_name(
                builder,
                prefix_db_zk_path=prefix_db_zk_path,
                migrating_database=database_name,
            )
        _create_query_node(ctx, builder, prefix_db_zk_path, counter)
        _create_database_replica(ctx, builder, database_name, prefix_db_zk_path)

        if first_replica:
            _create_database_metadata_nodes(
                ctx,
                builder,
                database_name,
                prefix_db_zk_path,
            )

        builder.commit()


def migrate_as_non_first_replica(ctx: Context, migrating_database: str) -> None:
    logging.info("call migrate_as_non_first_replica")

    prefix_db_zk_path = _default_db_zk_path(migrating_database)

    with zk_client(ctx) as zk:
        metadata_non_repl_db = parse_database_metadata(migrating_database)
        tables = list_tables(ctx, database_name=migrating_database)

        _check_tables_consistent(ctx, migrating_database, tables)

        counter = _generate_counter(ctx, zk, prefix_db_zk_path)

        builder = ZKTransactionBuilder(ctx, zk)

        _create_query_node(ctx, builder, prefix_db_zk_path, counter)
        _create_database_replica(
            ctx,
            builder,
            migrating_database,
            prefix_db_zk_path=prefix_db_zk_path,
        )

        _detach_tables(ctx, tables, permanently=False)
        _detach_dbs(ctx, dbs=[migrating_database])

        builder.commit()

    metadata_non_repl_db.set_replicated()
    was_changed = _change_tables_uuid(ctx, tables, migrating_database)

    if was_changed:
        logging.info(
            f"Table UUID was changed. Database {migrating_database} was detached. Need restart Clickhouse."
        )
    else:
        _attach_dbs(ctx, dbs=[migrating_database])


def _get_host_id(ctx: Context, migrating_database: str, replica: str) -> str:
    host_name = replica

    logging.debug("host_name={}", host_name)

    query = f"""
        SELECT uuid FROM system.databases WHERE database='{migrating_database}'
    """
    rows = execute_query(ctx, query, echo=True, format_=OutputFormat.JSON)
    database_uuid = rows["data"][0]["uuid"]

    ch_server_config = get_clickhouse_config(ctx)
    tcp_port = ch_server_config.ports[ClickhousePort.TCP]

    result = f"{escape_for_zookeeper(host_name)}:{tcp_port}:{database_uuid}"
    logging.debug("_get_host_id={}", result)

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

        builder = ZKTransactionBuilder(ctx, zk)

        builder.create_node(path=prefix_db_zk_path, value="DatabaseReplicated")
        builder.create_node(path=f"{prefix_db_zk_path}/log")
        builder.create_node(path=f"{prefix_db_zk_path}/replicas")
        builder.create_node(path=f"{prefix_db_zk_path}/counter")
        builder.create_node(path=f"{prefix_db_zk_path}/counter/cnt-")

        builder.delete_node(path=f"{prefix_db_zk_path}/counter/cnt-")

        builder.create_node(path=f"{prefix_db_zk_path}/metadata")

        max_log_ptr = "1"
        builder.create_node(path=f"{prefix_db_zk_path}/max_log_ptr", value=max_log_ptr)

        data_logs_to_keep = "1000"
        builder.create_node(
            path=f"{prefix_db_zk_path}/logs_to_keep", value=data_logs_to_keep
        )

        builder.commit()


def _create_first_replica_database_name(
    builder: ZKTransactionBuilder,
    prefix_db_zk_path: str,
    migrating_database: str,
) -> None:
    logging.debug("call create_first_replica_database_name.")

    builder.create_node(
        path=f"{prefix_db_zk_path}/first_replica_database_name",
        value=migrating_database,
    )


def _create_query_node(
    ctx: Context, builder: ZKTransactionBuilder, prefix_db_zk_path: str, counter: str
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

    builder.create_node(
        path=f"{prefix_db_zk_path}/log/query-{counter}", value=data_log_queue
    )

    shard, replica = get_shard_and_replica_from_macros(ctx)
    builder.create_node(
        path=f"{prefix_db_zk_path}/log/query-{counter}/committed",
        value=f"{shard}|{replica}",
    )
    builder.create_node(path=f"{prefix_db_zk_path}/log/query-{counter}/active")
    builder.create_node(path=f"{prefix_db_zk_path}/log/query-{counter}/finished")
    builder.create_node(path=f"{prefix_db_zk_path}/log/query-{counter}/synced")


def _create_database_replica(
    ctx: Context,
    builder: ZKTransactionBuilder,
    migrating_database: str,
    prefix_db_zk_path: str,
) -> None:
    """
    Create replica nodes for Replicated database.
    https://github.com/ClickHouse/ClickHouse/blob/master/src/Databases/DatabaseReplicated.cpp#L714
    """
    logging.debug("call create_database_replica")

    shard, replica = get_shard_and_replica_from_macros(ctx)

    builder.create_node(
        path=f"{prefix_db_zk_path}/log/query-0000000001/finished/{shard}|{replica}",
        value="0",
    )

    replica_node = f"{prefix_db_zk_path}/replicas/{shard}|{replica}"
    builder.create_node(
        path=replica_node, value=_get_host_id(ctx, migrating_database, replica)
    )

    query = """
        SELECT serverUUID() as id
    """
    rows = execute_query(ctx, query, echo=True, format_=OutputFormat.JSON)
    server_uuid = rows["data"][0]["id"]
    logging.debug("rows={}, server_uuid={}", rows, server_uuid)

    builder.create_node(path=f"{replica_node}/active", value=server_uuid)
    builder.create_node(path=f"{replica_node}/digest", value="0")
    builder.create_node(path=f"{replica_node}/log_ptr", value="0")
    builder.create_node(path=f"{replica_node}/max_log_ptr_at_creation", value="1")


def _create_database_metadata_nodes(
    ctx: Context,
    builder: ZKTransactionBuilder,
    migrating_database: str,
    prefix_db_zk_path: str,
) -> None:
    query = f"""
        SELECT name, create_table_query, metadata_path FROM system.tables WHERE database='{migrating_database}'
    """
    rows = execute_query(ctx, query, echo=True, format_=OutputFormat.JSON)

    for table in rows["data"]:
        table_name = table["name"]
        metadata_path = table["metadata_path"]

        if match_ch_version(ctx, "25.1"):
            metadata_path = CLICKHOUSE_PATH + "/" + metadata_path

        with open(metadata_path, "r", encoding="utf-8") as metadata_file:
            local_table_metadata = metadata_file.read()

            builder.create_node(
                path=f"{prefix_db_zk_path}/metadata/{table_name}",
                value=local_table_metadata,
            )

            logging.debug(
                "add table metadata to txn: {}",
                local_table_metadata,
            )


def _detach_dbs(ctx: Context, dbs: list[str]) -> None:
    for db in dbs:
        query = f"""
            DETACH DATABASE {db}
        """
        execute_query(
            ctx,
            query,
            echo=True,
        )


def _detach_tables(ctx: Context, tables: list[TableInfo], permanently: bool) -> None:
    for table in tables:
        detach_table(ctx, table["database"], table["name"], permanently)


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


def _change_tables_uuid(
    ctx: Context, tables_info: list[TableInfo], database_name: str
) -> bool:
    was_changed = False
    for table in tables_info:
        table_local_metadata_path = table["metadata_path"]

        table_name = table["name"]
        database_name = table["database"]
        old_table_uuid = table["uuid"]

        zk_metadata_path = f"/clickhouse/{database_name}/metadata/{table_name}"
        zk_table_metadata = get_zk_node(ctx, zk_metadata_path)

        zk_table_uuid = metadata.parse_uuid(zk_table_metadata)

        logging.debug(
            "Table {} has old_table_uuid={}, zk_table_uuid={}",
            table_name,
            old_table_uuid,
            zk_table_uuid,
        )

        if zk_table_uuid == old_table_uuid:
            logging.debug(
                "Table {}.{} has equal uuid. Don't need to change uuid.",
                database_name,
                table_name,
            )
            continue

        was_changed = True

        change_table_uuid(
            ctx,
            database=database_name,
            table=table_name,
            engine=table["engine"],
            new_local_uuid=zk_table_uuid,
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
    metadata_db = parse_database_metadata(database)
    if metadata_db.database_engine != DatabaseEngine.ATOMIC:
        raise RuntimeError(
            f"Database {database} has engine {metadata_db.database_engine}. Migration to Replicated from Atomic only is supported."
        )

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
    metadata_repl_db = parse_database_metadata(database)
    if metadata_repl_db.database_engine != DatabaseEngine.REPLICATED:
        raise RuntimeError(
            f"Database {database} has engine {metadata_repl_db.database_engine}. Migration to Atomic from Replicated only is supported."
        )

    with AttacherContext(ctx, database) as _:
        try:
            zookeeper_path = metadata_repl_db.zookeeper_path
            metadata_repl_db.set_atomic()

            if clean_zookeeper:
                logging.debug(
                    "Set clean_zookeeper - delete zookeeper nodes {}",
                    zookeeper_path,
                )
                if zookeeper_path:
                    delete_zk_node(ctx, zookeeper_path)

        except Exception as ex:
            logging.error("Failed set atomic in metadata: {}", ex)
            raise ex

        logging.info("Metadata {} was updated to Atomic", database)
