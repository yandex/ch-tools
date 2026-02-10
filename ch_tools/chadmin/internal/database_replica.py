"""
Database replica management for ClickHouse Replicated databases.

Handles ZooKeeper structure creation, replica registration, and metadata synchronization
for Replicated database engine. Manages database nodes, query logs, replica tracking,
and table metadata storage in ZooKeeper.
"""

from typing import Optional, Tuple

from click import Context
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError

from ch_tools.chadmin.internal.clickhouse_disks import CLICKHOUSE_PATH
from ch_tools.chadmin.internal.system import match_ch_version
from ch_tools.chadmin.internal.utils import execute_query, replace_macros
from ch_tools.chadmin.internal.zookeeper import (
    ZKTransactionBuilder,
    escape_for_zookeeper,
    format_path,
    unescape_from_zookeeper,
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


def check_database_exists_in_zk(
    ctx: Context, database_name: str, db_replica_path: Optional[str] = None
) -> bool:
    zk_path = db_replica_path or f"{DEFAULT_ZK_ROOT}/{database_name}"
    with zk_client(ctx) as zk:
        return zk.exists(format_path(ctx, zk_path)) is not None


def get_default_table_in_db_path(database_name: str, table_name: str) -> str:
    """Get ZooKeeper path for table metadata"""
    escaped_table_name = escape_for_zookeeper(table_name)
    return (
        f"{DEFAULT_ZK_ROOT}/{database_name}/{ZK_METADATA_SUBPATH}/{escaped_table_name}"
    )


def get_tables_metadata(ctx: Context, database_name: str) -> dict[str, str]:
    """Retrieve table metadata from ZooKeeper, returns dict mapping table names to CREATE statements."""
    zk_tables_metadata: dict[str, str] = {}

    with zk_client(ctx) as zk:
        zk_metadata_path = f"{DEFAULT_ZK_ROOT}/{database_name}/{ZK_METADATA_SUBPATH}"
        children = zk.get_children(zk_metadata_path)
        if not children:
            return zk_tables_metadata

        for escaped_table_name in children:
            table_metadata_path = f"{zk_metadata_path}/{escaped_table_name}"
            # Unescape table name to get original name for dictionary key
            table_name = unescape_from_zookeeper(escaped_table_name)
            try:
                metadata_data = zk.get(table_metadata_path)
                if metadata_data and metadata_data[0]:
                    zk_tables_metadata[table_name] = metadata_data[0].decode().strip()
                else:
                    # Node exists but is empty or unreadable; log for easier diagnosis of partial failures.
                    logging.warning(
                        "Empty or missing ZooKeeper metadata for table %s at path %s",
                        table_name,
                        table_metadata_path,
                    )
            except NoNodeError:
                logging.warning(
                    "ZooKeeper metadata node for table %s was removed concurrently at path %s",
                    table_name,
                    table_metadata_path,
                )

    return zk_tables_metadata


def system_database_drop_replica(
    ctx: Context, database_zk_path: str, replica: str, dry_run: bool = False
) -> None:
    """Perform SYSTEM DROP DATABASE REPLICA query."""
    timeout = ctx.obj["config"]["clickhouse"]["drop_replica_timeout"]
    query = f"SYSTEM DROP DATABASE REPLICA '{replica}' FROM ZKPATH '{database_zk_path}'"
    execute_query(ctx, query, timeout=timeout, echo=True, dry_run=dry_run, format_=None)


def supports_system_restore_database_replica(ctx: Context) -> bool:
    """Check if ClickHouse version supports SYSTEM RESTORE DATABASE REPLICA command."""
    return match_ch_version(ctx, "25.8")


def system_restore_database_replica(ctx: Context, database_name: str) -> None:
    """Restore database replica using SYSTEM RESTORE DATABASE REPLICA command."""
    # Wrap database name in backticks to handle special characters
    query = f"SYSTEM RESTORE DATABASE REPLICA `{database_name}`"
    execute_query(ctx, query, echo=True, format_=None)
    logging.info(
        f"Successfully restored replica for {database_name} using SYSTEM RESTORE DATABASE REPLICA"
    )


def _restore_replica_fallback(
    ctx: Context,
    database_name: str,
    db_replica_path: Optional[str] = None,
) -> None:
    """Fallback method for restoring database replica on older ClickHouse versions (< 25.8)."""
    logging.info(
        f"Using legacy ZookeeperDatabaseManager for restore of {database_name}"
    )

    zk_manager = ZookeeperDatabaseManager(ctx)

    # Determine if this is the first replica
    first_replica = not check_database_exists_in_zk(ctx, database_name, db_replica_path)

    if first_replica:
        logging.info(
            f"Restoring {database_name} as first replica (creating database structure)"
        )
        try:
            zk_manager.create_database_structure(database_name, db_replica_path)
        except NodeExistsError:
            logging.info(
                "Database nodes created concurrently, continuing as non-first replica"
            )
            first_replica = False
    else:
        logging.info(f"Restoring {database_name} as non-first replica")

    # Create replica nodes
    zk_manager.create_replica_nodes(database_name, first_replica, db_replica_path)
    logging.info(
        f"Successfully restored replica for {database_name} using fallback method"
    )


class ZookeeperDatabaseManager:
    """
    Manages ZooKeeper structure for Replicated databases.
    DEPRECATED: Use SYSTEM RESTORE DATABASE REPLICA for CH >= 25.8

    ZooKeeper Structure for Replicated Database
    ===========================================

    Root Database Nodes ({zookeeper_path}/):
    ----------------------------------------
    /                                       # Root path for database
    ├── [value: "DatabaseReplicated"]      # Database engine type marker
    ├── log/                                # DDL replication log
    │   └── query-{counter}/               # Individual DDL log entry
    │       ├── [value: YAML]              # DDL entry (query, hosts, initiator)
    │       ├── committed                  # Marks entry as committed (persistent)
    │       ├── try                        # Ephemeral node for execution attempt
    │       ├── active/                    # Active executors directory
    │       ├── finished/                  # Execution completion markers
    │       │   └── {shard}|{replica}     # Execution status per replica
    │       └── synced/                    # Synchronization markers
    │           └── {shard}|{replica}     # Sync status per replica
    ├── replicas/                          # All database replicas
    │   └── {shard}|{replica}/            # Individual replica node
    │       ├── [value: host_id]          # Format: {escaped_host}:{port}:{db_uuid}
    │       ├── active                    # Ephemeral node (created by DDLWorker)
    │       ├── digest                    # Metadata hash (UInt64)
    │       ├── log_ptr                   # Last processed log entry number
    │       ├── max_log_ptr_at_creation  # max_log_ptr value at replica creation
    │       └── replica_group             # Replica group name (for filtering)
    ├── counter/                           # Atomic counter for log entry IDs
    │   └── cnt-{seq}                     # Ephemeral sequential nodes
    ├── metadata/                          # Table metadata storage
    │   └── {escaped_table_name}          # CREATE TABLE statement per table
    ├── max_log_ptr                       # Latest log entry number (UInt32)
    ├── logs_to_keep                      # Number of log entries to retain (UInt32)
    └── first_replica_database_name       # Database name on first replica

    Replica Registration Process:
    -----------------------------
    1. First replica creates database structure (create_database_structure)
    2. Each replica creates its own nodes (create_replica_nodes):
       - Generates unique counter for log entry
       - Creates initial log entry (query-0000000001)
       - Registers replica with host_id
       - Creates tracking nodes (digest, log_ptr, etc.)
    3. DDLWorker creates ephemeral 'active' node on startup

    Name Escaping:
    -------------
    - Table names: escaped using escapeForFileName() logic
    - Hostnames: escaped using same logic for ZooKeeper compatibility
    - Replica names: format is "{shard}|{replica}" (pipe separator)

    References:
    ----------
    - ClickHouse source: src/Databases/DatabaseReplicated.cpp
    - Documentation: docs/en/engines/database-engines/replicated.md
    """

    def __init__(self, ctx: Context):
        self.ctx = ctx

    def create_database_structure(
        self, database_name: str, db_replica_path: Optional[str] = None
    ) -> None:
        """
        Create ZooKeeper structure for Replicated database with all required nodes.

        Creates the following ZooKeeper structure:
        {zk_path}/
        ├── [value: "DatabaseReplicated"]      # Database type marker
        ├── log/                                # DDL replication log
        ├── replicas/                           # All database replicas
        ├── counter/                            # Atomic counter for log entry IDs
        ├── metadata/                           # Table CREATE statements
        ├── max_log_ptr                        # Latest log entry number (default: "1")
        └── logs_to_keep                       # Number of log entries to retain (default: "1000")

        Note: The counter/cnt- node is created and immediately deleted to initialize
        the sequence counter, ensuring log entry numbers start from 1 (not 0).
        """
        with zk_client(self.ctx) as zk:
            prefix_db_zk_path = db_replica_path or self._get_default_db_path(
                database_name
            )

            if not db_replica_path:
                if not zk.exists(format_path(self.ctx, DEFAULT_ZK_ROOT)):
                    zk.create(format_path(self.ctx, DEFAULT_ZK_ROOT), makepath=True)

            with ZKTransactionBuilder(self.ctx, zk) as builder:
                # Create main database node
                builder.create_node(path=prefix_db_zk_path, value="DatabaseReplicated")
                builder.create_node(path=f"{prefix_db_zk_path}/{ZK_LOG_SUBPATH}")
                builder.create_node(path=f"{prefix_db_zk_path}/{ZK_REPLICAS_SUBPATH}")
                builder.create_node(path=f"{prefix_db_zk_path}/{ZK_COUNTER_SUBPATH}")

                # Create and delete counter node to initialize sequence
                builder.create_node(
                    path=f"{prefix_db_zk_path}/{ZK_COUNTER_SUBPATH}/cnt-"
                )
                builder.delete_node(
                    path=f"{prefix_db_zk_path}/{ZK_COUNTER_SUBPATH}/cnt-"
                )

                builder.create_node(path=f"{prefix_db_zk_path}/{ZK_METADATA_SUBPATH}")
                builder.create_node(
                    path=f"{prefix_db_zk_path}/{ZK_MAX_LOG_PTR_SUBPATH}",
                    value=DEFAULT_MAX_LOG_PTR,
                )
                builder.create_node(
                    path=f"{prefix_db_zk_path}/{ZK_LOGS_TO_KEEP_SUBPATH}",
                    value=DEFAULT_LOGS_TO_KEEP,
                )

    def create_replica_nodes(
        self,
        database_name: str,
        first_replica: bool,
        db_replica_path: Optional[str] = None,
    ) -> None:
        """
        Register database replica in ZooKeeper with replica-specific nodes.

        Creates the following structure for each replica:
        {zk_path}/replicas/{shard}|{replica}/
        ├── [value: host_id]               # Format: {escaped_host}:{port}:{db_uuid}
        ├── active                         # Ephemeral node (created by DDLWorker)
        ├── digest                         # Metadata hash (UInt64, initially "0")
        ├── log_ptr                        # Last processed log entry (initially "0")
        └── max_log_ptr_at_creation       # max_log_ptr value at replica creation

        Also creates initial log entry:
        {zk_path}/log/query-{counter}/
        ├── [value: YAML template]         # Empty DDL entry
        ├── committed                      # Marks entry as committed
        ├── active/                        # Directory for active executors
        ├── finished/                      # Directory for completion markers
        │   └── {shard}|{replica}         # Initial completion marker
        └── synced/                        # Directory for sync markers

        For first replica only:
        - Creates first_replica_database_name node
        - Stores table metadata in {zk_path}/metadata/
        """
        prefix_db_zk_path = db_replica_path or self._get_default_db_path(database_name)

        with zk_client(self.ctx) as zk:
            counter = self._generate_counter(zk, prefix_db_zk_path)

            with ZKTransactionBuilder(self.ctx, zk) as builder:
                if first_replica:
                    self._create_first_replica_name_node(
                        builder, prefix_db_zk_path, database_name
                    )

                self._create_query_log_entry(builder, prefix_db_zk_path, counter)
                self._create_replica_registration(
                    builder, database_name, prefix_db_zk_path
                )

                if first_replica:
                    self._create_table_metadata_nodes(
                        builder, database_name, prefix_db_zk_path
                    )

    def _generate_counter(self, zk: KazooClient, db_zk_path: str) -> str:
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

    def _create_first_replica_name_node(
        self, builder: ZKTransactionBuilder, prefix_db_zk_path: str, database_name: str
    ) -> None:
        """
        Store original database name for first replica.

        Creates node: {zk_path}/first_replica_database_name = database_name

        This node is used for introspection purposes, as technically the database
        may have different names on different replicas (though this is not common).
        """
        builder.create_node(
            path=f"{prefix_db_zk_path}/{ZK_FIRST_REPLICA_DB_NAME_SUBPATH}",
            value=database_name,
        )

    def _create_query_log_entry(
        self, builder: ZKTransactionBuilder, prefix_db_zk_path: str, counter: str
    ) -> None:
        """
        Create initial query log entry in ZooKeeper.

        Creates structure:
        {zk_path}/log/query-{counter}/
        ├── [value: YAML template]     # Empty DDL entry
        ├── committed                  # Marks entry as committed
        │   └── [value: {shard}|{replica}]
        ├── active/                    # Directory for active executors
        ├── finished/                  # Directory for completion markers
        └── synced/                    # Directory for sync markers

        The initial entry (query-0000000001) is created empty to:
        1. Initialize the log sequence
        2. Ensure log_ptr starts from 1 (not 0)
        3. Provide a baseline for replica synchronization
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

    def _create_replica_registration(
        self,
        builder: ZKTransactionBuilder,
        database_name: str,
        prefix_db_zk_path: str,
    ) -> None:
        """
        Register replica in ZooKeeper with host information and tracking nodes.

        Creates:
        1. Completion marker for initial log entry:
           {zk_path}/log/query-0000000001/finished/{shard}|{replica} = "0"

        2. Replica node with host_id:
           {zk_path}/replicas/{shard}|{replica} = {escaped_host}:{port}:{db_uuid}

        3. Replica tracking nodes:
           - active: Server UUID (ephemeral, recreated by DDLWorker)
           - digest: Metadata hash (initially "0")
           - log_ptr: Last processed log entry (initially "0")
           - max_log_ptr_at_creation: max_log_ptr value at replica creation
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

    def _create_table_metadata_nodes(
        self,
        builder: ZKTransactionBuilder,
        database_name: str,
        prefix_db_zk_path: str,
    ) -> None:
        """
        Store table metadata in ZooKeeper for first replica.

        Creates nodes:
        {zk_path}/metadata/{escaped_table_name} = CREATE TABLE statement

        Table names are escaped using escapeForFileName() logic to handle
        special characters in ZooKeeper node names. This ensures compatibility
        with ClickHouse's internal naming conventions.

        Only called for the first replica to initialize the metadata storage.
        Subsequent replicas will read metadata from ZooKeeper during recovery.
        """
        query = """
            SELECT name, create_table_query, metadata_path FROM system.tables WHERE database='{{ database_name }}'
        """
        rows = execute_query(
            self.ctx, query, database_name=database_name, format_=OutputFormat.JSON
        )

        for table in rows["data"]:
            table_name = table["name"]
            metadata_path = table["metadata_path"]

            if match_ch_version(self.ctx, "25.1"):
                metadata_path = CLICKHOUSE_PATH + "/" + metadata_path

            with open(metadata_path, "r", encoding="utf-8") as metadata_file:
                local_table_metadata = metadata_file.read()

                # Escape table name for ZooKeeper node (same as ClickHouse escapeForFileName)
                escaped_table_name = escape_for_zookeeper(table_name)

                builder.create_node(
                    path=f"{prefix_db_zk_path}/{ZK_METADATA_SUBPATH}/{escaped_table_name}",
                    value=local_table_metadata,
                )

    def _get_host_id(self, database_name: str, replica: str) -> str:
        """
        Generate host ID for replica registration.

        Format: {escaped_hostname}:{tcp_port}:{database_uuid}

        Example: "host%2Dname:9000:550e8400-e29b-41d4-a716-446655440000"
        """
        query = """
            SELECT uuid FROM system.databases WHERE database='{{ database_name }}'
        """
        rows = execute_query(
            self.ctx, query, database_name=database_name, format_=OutputFormat.JSON
        )
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
        """
        Extract shard and replica names from ClickHouse macros.

        Returns tuple: (shard_name, replica_name)

        These values are used to form the replica identifier in ZooKeeper
        as "{shard}|{replica}" (pipe-separated format).

        Raises RuntimeError if required macros are not defined.
        """
        macros = get_macros(self.ctx)

        missing = [macro for macro in ["shard", "replica"] if macro not in macros]
        if missing:
            raise RuntimeError(f"Missing macros: {missing}")

        shard = replace_macros("{shard}", macros)
        replica = replace_macros("{replica}", macros)

        return shard, replica

    def _get_default_db_path(self, database_name: str) -> str:
        """Get default ZooKeeper path for database."""
        return f"{DEFAULT_ZK_ROOT}/{database_name}"
