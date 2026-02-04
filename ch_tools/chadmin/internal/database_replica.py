"""
Database replica management for ClickHouse Replicated databases.

Handles ZooKeeper structure creation, replica registration, and metadata synchronization
for Replicated database engine. Manages database nodes, query logs, replica tracking,
and table metadata storage in ZooKeeper.
"""

from typing import Optional, Tuple

from click import Context
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

from ch_tools.chadmin.internal.clickhouse_disks import CLICKHOUSE_PATH
from ch_tools.chadmin.internal.system import match_ch_version
from ch_tools.chadmin.internal.utils import execute_query, replace_macros
from ch_tools.chadmin.internal.zookeeper import (
    ZKTransactionBuilder,
    escape_for_zookeeper,
    format_path,
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


class ZookeeperDatabaseManager:
    """
    Manages ZooKeeper structure for Replicated databases.

    Handles creation of database nodes, replica registration,
    and metadata synchronization in ZooKeeper.
    """

    def __init__(self, ctx: Context):
        self.ctx = ctx

    def get_default_table_in_db_path(self, database_name: str, table_name: str) -> str:
        return f"{DEFAULT_ZK_ROOT}/{database_name}/{ZK_METADATA_SUBPATH}/{table_name}"

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

            with ZKTransactionBuilder(self.ctx, zk) as builder:
                if first_replica:
                    self.create_first_replica_name_node(
                        builder, prefix_db_zk_path, database_name
                    )

                self.create_query_log_entry(builder, prefix_db_zk_path, counter)
                self.create_replica_registration(
                    builder, database_name, prefix_db_zk_path
                )

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

        Reference link: https://github.com/ClickHouse/ClickHouse/blob/master/src/Databases/DatabaseReplicatedWorker.cpp#L380
        Reference code: DatabaseReplicatedDDLWorker::enqueueQueryImpl
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
        Reference link: https://github.com/ClickHouse/ClickHouse/blob/master/src/Databases/DatabaseReplicated.cpp#L800
        Reference code: DatabaseReplicated::createReplicaNodesInZooKeeper
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

                builder.create_node(
                    path=f"{prefix_db_zk_path}/{ZK_METADATA_SUBPATH}/{table_name}",
                    value=local_table_metadata,
                )

    def _get_host_id(self, database_name: str, replica: str) -> str:
        """
        Generate host ID for replica registration.

        Format: {escaped_hostname}:{tcp_port}:{database_uuid}
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


def system_database_drop_replica(
    ctx: Context, database_zk_path: str, replica: str, dry_run: bool = False
) -> None:
    """
    Perform "SYSTEM DROP DATABASE REPLICA" query.
    """
    timeout = ctx.obj["config"]["clickhouse"]["drop_replica_timeout"]
    query = f"SYSTEM DROP DATABASE REPLICA '{replica}' FROM ZKPATH '{database_zk_path}'"
    execute_query(ctx, query, timeout=timeout, echo=True, dry_run=dry_run, format_=None)


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
