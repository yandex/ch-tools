"""
Database migration utilities for ClickHouse.

Handles migration between Atomic and Replicated database engines,
including ZooKeeper structure management and replica restoration.
"""

from operator import itemgetter
from typing import Any, List, Literal, Optional, Tuple

from click import ClickException, Context
from kazoo.client import KazooClient
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

# ZooKeeper path constants
DEFAULT_ZK_ROOT = "/clickhouse"
ZK_LOG_SUBPATH = "log"
ZK_REPLICAS_SUBPATH = "replicas"
ZK_COUNTER_SUBPATH = "counter"
ZK_METADATA_SUBPATH = "metadata"
ZK_MAX_LOG_PTR_SUBPATH = "max_log_ptr"
ZK_LOGS_TO_KEEP_SUBPATH = "logs_to_keep"
ZK_FIRST_REPLICA_DB_NAME_SUBPATH = "first_replica_database_name"

# Default values for ZK nodes
DEFAULT_MAX_LOG_PTR = "1"
DEFAULT_LOGS_TO_KEEP = "1000"
INITIAL_LOG_ENTRY_ID = "0000000001"

# Query log template for Replicated database
QUERY_LOG_TEMPLATE = """version: 1
query: 
hosts: []
initiator: 
"""  # noqa: W291


class AttacherContext:
    """
    Context manager for database detach/attach operations.

    Detaches database on enter and attaches it back on exit.
    """

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
                f"Exception in AttacherContext: {exc_type.__name__}: {exc_value}"
            )
        return False


class ZKTransactionBuilder:
    """
    Builder for ZooKeeper transactions with path tracking.

    Provides methods to create and delete nodes within a transaction,
    tracks all operations, and validates results on commit.
    """

    def __init__(self, ctx: Context, zk: KazooClient) -> None:
        self.ctx = ctx
        self.txn = zk.transaction()
        self.path_to_nodes: List[str] = []

    def create_node(self, path: str, value: str = "") -> None:
        """Add create operation to transaction."""
        self.path_to_nodes.append(path)
        self.txn.create(path=format_path(self.ctx, path), value=value.encode())

    def delete_node(self, path: str) -> None:
        """Add delete operation to transaction."""
        self.path_to_nodes.append(path)
        self.txn.delete(path=format_path(self.ctx, path))

    def commit(self) -> None:
        """Execute transaction and validate results."""
        result = self.txn.commit()

        for path, status in zip(self.path_to_nodes, result):
            logging.debug(f"Transaction result for {path}: {status}")

        self.path_to_nodes = []
        self._check_result_txn(result)

    @staticmethod
    def _check_result_txn(results: List) -> None:
        """Validate transaction results and raise on errors."""
        for result in results:
            if isinstance(result, NodeExistsError):
                raise NodeExistsError()
            if isinstance(result, Exception):
                logging.error(f"Transaction error: {result}, type={type(result)}")
                raise result


class ZookeeperDatabaseManager:
    """
    Manages ZooKeeper structure for Replicated databases.

    Handles creation of database nodes, replica registration,
    and metadata synchronization in ZooKeeper.
    """

    def __init__(self, ctx: Context):
        self.ctx = ctx

    def create_database_structure(
        self, database_name: str, db_replica_path: Optional[str] = None
    ) -> None:
        """
        Create ZooKeeper structure for Replicated database.

        Creates the following structure:
        - /{db_path} - root node with "DatabaseReplicated" value
        - /{db_path}/log - query log
        - /{db_path}/replicas - replica nodes
        - /{db_path}/counter - counter for log entries
        - /{db_path}/metadata - table metadata storage
        - /{db_path}/max_log_ptr - maximum log pointer
        - /{db_path}/logs_to_keep - number of logs to retain

        Reference: https://github.com/ClickHouse/ClickHouse/blob/master/src/Databases/DatabaseReplicated.cpp#L581
        """
        with zk_client(self.ctx) as zk:
            prefix_db_zk_path = db_replica_path or self.get_default_db_path(
                database_name
            )

            if not db_replica_path:
                if not zk.exists(format_path(self.ctx, DEFAULT_ZK_ROOT)):
                    zk.create(format_path(self.ctx, DEFAULT_ZK_ROOT), makepath=True)

            builder = ZKTransactionBuilder(self.ctx, zk)

            # Create main database node
            builder.create_node(path=prefix_db_zk_path, value="DatabaseReplicated")
            builder.create_node(path=f"{prefix_db_zk_path}/{ZK_LOG_SUBPATH}")
            builder.create_node(path=f"{prefix_db_zk_path}/{ZK_REPLICAS_SUBPATH}")
            builder.create_node(path=f"{prefix_db_zk_path}/{ZK_COUNTER_SUBPATH}")

            # Create and delete counter node to initialize sequence
            builder.create_node(path=f"{prefix_db_zk_path}/{ZK_COUNTER_SUBPATH}/cnt-")
            builder.delete_node(path=f"{prefix_db_zk_path}/{ZK_COUNTER_SUBPATH}/cnt-")

            builder.create_node(path=f"{prefix_db_zk_path}/{ZK_METADATA_SUBPATH}")
            builder.create_node(
                path=f"{prefix_db_zk_path}/{ZK_MAX_LOG_PTR_SUBPATH}",
                value=DEFAULT_MAX_LOG_PTR,
            )
            builder.create_node(
                path=f"{prefix_db_zk_path}/{ZK_LOGS_TO_KEEP_SUBPATH}",
                value=DEFAULT_LOGS_TO_KEEP,
            )

            builder.commit()

    def create_replica_nodes(
        self,
        database_name: str,
        first_replica: bool,
        db_replica_path: Optional[str] = None,
    ) -> None:
        """
        Register database replica in ZooKeeper.

        Creates replica-specific nodes and query log entries.
        For first replica, also creates database name and metadata nodes.
        """
        prefix_db_zk_path = db_replica_path or self.get_default_db_path(database_name)

        with zk_client(self.ctx) as zk:
            counter = self.generate_counter(zk, prefix_db_zk_path)
            builder = ZKTransactionBuilder(self.ctx, zk)

            if first_replica:
                self.create_first_replica_name_node(
                    builder, prefix_db_zk_path, database_name
                )

            self.create_query_log_entry(builder, prefix_db_zk_path, counter)
            self.create_replica_registration(builder, database_name, prefix_db_zk_path)

            if first_replica:
                self.create_table_metadata_nodes(
                    builder, database_name, prefix_db_zk_path
                )

            builder.commit()

    def get_tables_metadata(self, database_name: str) -> dict[str, str]:
        """
        Retrieve table metadata from ZooKeeper for Replicated database.

        Returns dict mapping table names to their CREATE statements.
        """
        zk_tables_metadata: dict[str, str] = {}

        with zk_client(self.ctx) as zk:
            zk_metadata_path = (
                f"{DEFAULT_ZK_ROOT}/{database_name}/{ZK_METADATA_SUBPATH}"
            )
            children = zk.get_children(zk_metadata_path)
            if not children:
                return zk_tables_metadata

            for table_name in children:
                try:
                    table_metadata_path = f"{zk_metadata_path}/{table_name}"
                    metadata_data = zk.get(table_metadata_path)
                    if metadata_data and metadata_data[0]:
                        zk_tables_metadata[table_name] = (
                            metadata_data[0].decode().strip()
                        )
                except NoNodeError:
                    logging.warning(f"Table {table_name} metadata removed concurrently")

        return zk_tables_metadata

    def generate_counter(self, zk: KazooClient, db_zk_path: str) -> str:
        """Generate unique counter for log entries using ZK sequence."""
        path_counter = zk.create(
            format_path(self.ctx, f"{db_zk_path}/{ZK_COUNTER_SUBPATH}/cnt-"),
            sequence=True,
            ephemeral=True,
        )

        if path_counter is None:
            raise RuntimeError("Failed to generate counter in ZooKeeper")

        counter = path_counter[path_counter.rfind("-") + 1 :]
        assert len(counter) > 0

        logging.debug(f"Generated counter: {counter}")
        return counter

    def create_first_replica_name_node(
        self, builder: ZKTransactionBuilder, prefix_db_zk_path: str, database_name: str
    ) -> None:
        """Store original database name for first replica."""
        builder.create_node(
            path=f"{prefix_db_zk_path}/{ZK_FIRST_REPLICA_DB_NAME_SUBPATH}",
            value=database_name,
        )

    def create_query_log_entry(
        self, builder: ZKTransactionBuilder, prefix_db_zk_path: str, counter: str
    ) -> None:
        """
        Create query log entry in ZooKeeper.

        Reference: https://github.com/ClickHouse/ClickHouse/blob/master/src/Databases/DatabaseReplicatedWorker.cpp#L254
        """
        shard, replica = self._get_shard_and_replica()

        builder.create_node(
            path=f"{prefix_db_zk_path}/{ZK_LOG_SUBPATH}/query-{counter}",
            value=QUERY_LOG_TEMPLATE,
        )
        builder.create_node(
            path=f"{prefix_db_zk_path}/{ZK_LOG_SUBPATH}/query-{counter}/committed",
            value=f"{shard}|{replica}",
        )
        builder.create_node(
            path=f"{prefix_db_zk_path}/{ZK_LOG_SUBPATH}/query-{counter}/active"
        )
        builder.create_node(
            path=f"{prefix_db_zk_path}/{ZK_LOG_SUBPATH}/query-{counter}/finished"
        )
        builder.create_node(
            path=f"{prefix_db_zk_path}/{ZK_LOG_SUBPATH}/query-{counter}/synced"
        )

    def create_replica_registration(
        self,
        builder: ZKTransactionBuilder,
        database_name: str,
        prefix_db_zk_path: str,
    ) -> None:
        """
        Register replica in ZooKeeper.

        Creates replica node with host information and tracking nodes.
        Reference: https://github.com/ClickHouse/ClickHouse/blob/master/src/Databases/DatabaseReplicated.cpp#L714
        """
        shard, replica = self._get_shard_and_replica()

        # Mark initial log entry as finished for this replica
        builder.create_node(
            path=f"{prefix_db_zk_path}/{ZK_LOG_SUBPATH}/query-{INITIAL_LOG_ENTRY_ID}/finished/{shard}|{replica}",
            value="0",
        )

        # Create replica node with host ID
        replica_node = f"{prefix_db_zk_path}/{ZK_REPLICAS_SUBPATH}/{shard}|{replica}"
        builder.create_node(
            path=replica_node, value=self._get_host_id(database_name, replica)
        )

        # Create replica tracking nodes
        server_uuid = self._get_server_uuid()
        builder.create_node(path=f"{replica_node}/active", value=server_uuid)
        builder.create_node(path=f"{replica_node}/digest", value="0")
        builder.create_node(path=f"{replica_node}/log_ptr", value="0")
        builder.create_node(
            path=f"{replica_node}/max_log_ptr_at_creation", value=DEFAULT_MAX_LOG_PTR
        )

    def create_table_metadata_nodes(
        self,
        builder: ZKTransactionBuilder,
        database_name: str,
        prefix_db_zk_path: str,
    ) -> None:
        """Store table metadata in ZooKeeper for first replica."""
        query = f"""
            SELECT name, create_table_query, metadata_path FROM system.tables WHERE database='{database_name}'
        """
        rows = execute_query(self.ctx, query, format_=OutputFormat.JSON)

        for table in rows["data"]:
            table_name = table["name"]
            metadata_path = table["metadata_path"]

            if match_ch_version(self.ctx, "25.1"):
                metadata_path = CLICKHOUSE_PATH + "/" + metadata_path

            with open(metadata_path, "r", encoding="utf-8") as metadata_file:
                local_table_metadata = metadata_file.read()

                builder.create_node(
                    path=f"{prefix_db_zk_path}/{ZK_METADATA_SUBPATH}/{table_name}",
                    value=local_table_metadata,
                )

    def _get_host_id(self, database_name: str, replica: str) -> str:
        """
        Generate host ID for replica registration.

        Format: {escaped_hostname}:{tcp_port}:{database_uuid}
        """
        query = f"""
            SELECT uuid FROM system.databases WHERE database='{database_name}'
        """
        rows = execute_query(self.ctx, query, format_=OutputFormat.JSON)
        database_uuid = rows["data"][0]["uuid"]

        ch_server_config = get_clickhouse_config(self.ctx)
        tcp_port = ch_server_config.ports[ClickhousePort.TCP]

        result = f"{escape_for_zookeeper(replica)}:{tcp_port}:{database_uuid}"
        logging.debug(f"Generated host ID: {result}")
        return result

    def _get_server_uuid(self) -> str:
        """Get ClickHouse server UUID."""
        query = "SELECT serverUUID() as id"
        rows = execute_query(self.ctx, query, format_=OutputFormat.JSON)
        return rows["data"][0]["id"]

    def _get_shard_and_replica(self) -> Tuple[str, str]:
        """Extract shard and replica from macros."""
        macros = get_macros(self.ctx)

        missing = [macro for macro in ["shard", "replica"] if macro not in macros]
        if missing:
            raise RuntimeError(f"Missing macros: {missing}")

        shard = replace_macros("{shard}", macros)
        replica = replace_macros("{replica}", macros)

        return shard, replica

    def get_default_db_path(self, database_name: str) -> str:
        """Get default ZooKeeper path for database."""
        return f"{DEFAULT_ZK_ROOT}/{database_name}"


class DatabaseMigrator:
    """
    Handles database migration between Atomic and Replicated engines.

    Manages the migration process including metadata updates,
    table UUID synchronization, and replica coordination.
    """

    def __init__(self, ctx: Context):
        self.ctx = ctx
        self.zk_manager = ZookeeperDatabaseManager(ctx)

    def migrate_to_replicated(self, database: str) -> None:
        """
        Migrate Atomic database to Replicated engine.

        Determines if this is first or subsequent replica and
        executes appropriate migration path.
        """
        metadata_db = parse_database_metadata(database)
        if metadata_db.database_engine != DatabaseEngine.ATOMIC:
            raise RuntimeError(
                f"Database {database} has engine {metadata_db.database_engine}. "
                "Migration to Replicated from Atomic only is supported."
            )

        first_replica = self._try_create_database_nodes(database)

        if first_replica:
            self._migrate_first_replica(database)
        else:
            self._migrate_non_first_replica(database)

    def migrate_to_atomic(self, database: str, clean_zookeeper: bool) -> None:
        """
        Migrate Replicated database to Atomic engine.

        Updates local metadata and optionally cleans ZooKeeper nodes.
        """
        metadata_repl_db = parse_database_metadata(database)
        if metadata_repl_db.database_engine != DatabaseEngine.REPLICATED:
            raise RuntimeError(
                f"Database {database} has engine {metadata_repl_db.database_engine}. "
                "Migration to Atomic from Replicated only is supported."
            )

        with AttacherContext(self.ctx, database):
            zookeeper_path = metadata_repl_db.zookeeper_path
            metadata_repl_db.set_atomic()

            if clean_zookeeper and zookeeper_path:
                logging.info(f"Cleaning ZooKeeper nodes: {zookeeper_path}")
                delete_zk_node(self.ctx, zookeeper_path)

        logging.info(f"Database {database} migrated to Atomic")

    def _try_create_database_nodes(self, database: str) -> bool:
        """
        Attempt to create database nodes in ZooKeeper.

        Returns True if nodes were created (first replica),
        False if nodes already exist (subsequent replica).
        """
        try:
            create_database_nodes(self.ctx, database)
            return True
        except NodeExistsError:
            logging.debug("Database nodes exist, migrating as non-first replica")
            return False

    def _migrate_first_replica(self, database: str) -> None:
        """Migrate database as first replica."""
        logging.info(f"Migrating {database} as first replica")

        restore_replica(self.ctx, database, first_replica=True)
        self._update_local_metadata_first_replica(database)

    def _migrate_non_first_replica(self, database: str) -> None:
        """Migrate database as non-first replica."""
        logging.info(f"Migrating {database} as non-first replica")

        prefix_db_zk_path = self.zk_manager.get_default_db_path(database)

        with zk_client(self.ctx) as zk:
            metadata_non_repl_db = parse_database_metadata(database)
            tables = list_tables(self.ctx, database_name=database)

            self._check_tables_consistent(database, tables)

            counter = self.zk_manager.generate_counter(zk, prefix_db_zk_path)
            builder = ZKTransactionBuilder(self.ctx, zk)

            self.zk_manager.create_query_log_entry(builder, prefix_db_zk_path, counter)
            self.zk_manager.create_replica_registration(
                builder, database, prefix_db_zk_path
            )

            _detach_tables(self.ctx, tables, permanently=False)
            _detach_dbs(self.ctx, dbs=[database])

            builder.commit()

        metadata_non_repl_db.set_replicated()
        was_changed = self._sync_table_uuids(tables)

        if was_changed:
            logging.info(
                f"Table UUIDs changed for {database}. Database detached. Restart required."
            )
        else:
            _attach_dbs(self.ctx, dbs=[database])

    def _update_local_metadata_first_replica(self, database: str) -> None:
        """Update local database metadata to Replicated engine."""
        with AttacherContext(self.ctx, database):
            metadata_non_repl_db = parse_database_metadata(database)
            metadata_non_repl_db.set_replicated()

    def _check_tables_consistent(
        self, database_name: str, local_tables: list[TableInfo]
    ) -> None:
        """
        Verify local tables match ZooKeeper metadata.

        Checks that all local tables exist in ZK and schemas match.
        """
        zk_tables = self.zk_manager.get_tables_metadata(database_name)
        missing_in_zk = []
        schema_mismatches = []

        for table in local_tables:
            if table["name"] not in zk_tables:
                missing_in_zk.append(table["name"])
                continue

            if not self._compare_table_schemas(
                table["name"],
                read_local_table_metadata(self.ctx, table["metadata_path"]),
                zk_tables[table["name"]],
            ):
                schema_mismatches.append(table["name"])

        if missing_in_zk or schema_mismatches:
            error_msg = f"Database '{database_name}' tables inconsistent."
            if missing_in_zk:
                error_msg += f"\nMissing in ZK: {missing_in_zk}."
            if schema_mismatches:
                error_msg += f"\nSchema mismatches: {schema_mismatches}."

            logging.error(error_msg)
            logging.error(
                f"Local tables: {sorted(local_tables, key=itemgetter('name'))}"
            )
            logging.error(f"ZooKeeper tables: {sorted(zk_tables)}")

            raise RuntimeError(error_msg)

    def _compare_table_schemas(
        self, table_name: str, local_metadata: str, zk_metadata: str
    ) -> bool:
        """
        Compare table schemas ignoring UUID differences.

        Returns True if schemas match, False otherwise.
        """
        zk_metadata = zk_metadata.rstrip()
        local_metadata = local_metadata.rstrip()

        local_metadata = remove_uuid_from_metadata(local_metadata)
        zk_metadata = remove_uuid_from_metadata(zk_metadata)

        if local_metadata != zk_metadata:
            logging.warning(f"Table {table_name}: schema mismatch")
            return False

        logging.debug(f"Table {table_name}: schemas match")
        return True

    def _sync_table_uuids(self, tables_info: list[TableInfo]) -> bool:
        """
        Synchronize table UUIDs with ZooKeeper metadata.

        Returns True if any UUID was changed, False otherwise.
        """
        was_changed = False

        for table in tables_info:
            table_name = table["name"]
            database_name = table["database"]
            old_table_uuid = table["uuid"]

            zk_metadata_path = (
                f"{DEFAULT_ZK_ROOT}/{database_name}/{ZK_METADATA_SUBPATH}/{table_name}"
            )
            zk_table_metadata = get_zk_node(self.ctx, zk_metadata_path)
            zk_table_uuid = metadata.parse_uuid(zk_table_metadata)

            if zk_table_uuid == old_table_uuid:
                logging.debug(f"Table {database_name}.{table_name}: UUID unchanged")
                continue

            logging.info(
                f"Updating UUID for {database_name}.{table_name}: {old_table_uuid} -> {zk_table_uuid}"
            )
            was_changed = True

            change_table_uuid(
                self.ctx,
                database=database_name,
                table=table_name,
                engine=table["engine"],
                new_local_uuid=zk_table_uuid,
                old_table_uuid=old_table_uuid,
                table_local_metadata_path=table["metadata_path"],
                attached=False,
            )

        return was_changed


# Public API functions


def create_database_nodes(
    ctx: Context,
    database_name: str,
    db_replica_path: Optional[str] = None,
) -> None:
    """
    Create ZooKeeper structure for Replicated database.

    Public interface for creating database nodes in ZooKeeper.
    """
    zk_manager = ZookeeperDatabaseManager(ctx)
    zk_manager.create_database_structure(database_name, db_replica_path)


def is_database_exists(ctx: Context, database_name: str) -> bool:
    """Check if database exists in ClickHouse."""
    query = f"""
        SELECT 1 FROM system.databases WHERE database='{database_name}'
    """
    rows = execute_query(ctx, query, format_=OutputFormat.JSON)
    return len(rows["data"]) == 1


def migrate_database_to_atomic(
    ctx: Context, database: str, clean_zookeeper: bool
) -> None:
    """
    Migrate Replicated database to Atomic engine.

    Public interface for migration to Atomic.
    """
    migrator = DatabaseMigrator(ctx)
    migrator.migrate_to_atomic(database, clean_zookeeper)


def migrate_database_to_replicated(ctx: Context, database: str) -> None:
    """
    Migrate Atomic database to Replicated engine.

    Public interface for migration to Replicated.
    """
    migrator = DatabaseMigrator(ctx)
    migrator.migrate_to_replicated(database)


def restore_replica(
    ctx: Context,
    database_name: str,
    first_replica: bool,
    db_replica_path: Optional[str] = None,
) -> None:
    """
    Restore database replica in ZooKeeper.

    Creates necessary ZK nodes for replica registration.
    Used both for migration and manual replica restoration.
    """
    logging.info(f"Restoring replica for {database_name} (first={first_replica})")

    zk_manager = ZookeeperDatabaseManager(ctx)
    zk_manager.create_replica_nodes(database_name, first_replica, db_replica_path)


# Internal helper functions


def _detach_dbs(ctx: Context, dbs: list[str]) -> None:
    """Detach databases from ClickHouse."""
    for db in dbs:
        query = f"DETACH DATABASE {db}"
        execute_query(ctx, query, echo=True)


def _attach_dbs(ctx: Context, dbs: list[str]) -> None:
    """Attach databases to ClickHouse."""
    for db in dbs:
        query = f"ATTACH DATABASE {db}"
        execute_query(ctx, query, echo=True)


def _detach_tables(ctx: Context, tables: list[TableInfo], permanently: bool) -> None:
    """Detach tables from database."""
    for table in tables:
        detach_table(ctx, table["database"], table["name"], permanently)


# Deprecated function kept for backward compatibility
def create_temp_db(ctx: Context, migrating_database: str, temp_db: str) -> None:
    """
    Create temporary Replicated database.

    DEPRECATED: This function is kept for backward compatibility but is not used.
    """
    query = f"""
        CREATE DATABASE {temp_db} ON CLUSTER '{{cluster}}' ENGINE = Replicated('/clickhouse/{migrating_database}', '{{shard}}', '{{replica}}')
    """

    response = execute_query(ctx, query, echo=True, format_="JSON")

    ex_text = response.get("exception")
    if ex_text is not None:
        raise ClickException(ex_text)


# Deprecated function kept for backward compatibility
def get_shard_and_replica_from_macros(ctx: Context) -> Tuple[str, str]:
    """
    Get shard and replica from ClickHouse macros.

    DEPRECATED: Use ZookeeperDatabaseManager._get_shard_and_replica() instead.
    Kept for backward compatibility.
    """
    macros = get_macros(ctx)

    missing = [macro for macro in ["shard", "replica"] if macro not in macros]
    if missing:
        raise RuntimeError(f"Failed replace macros. {missing}")

    shard = replace_macros("{shard}", macros)
    replica = replace_macros("{replica}", macros)

    return shard, replica
