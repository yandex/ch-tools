"""
Database migration utilities for ClickHouse.

Handles migration between Atomic and Replicated database engines,
including ZooKeeper structure management and replica restoration.
"""

from operator import itemgetter
from typing import Any, Literal, Optional

from click import Context
from kazoo.exceptions import NodeExistsError

from ch_tools.chadmin.cli import metadata
from ch_tools.chadmin.cli.database_metadata import (
    DatabaseEngine,
    parse_database_metadata,
    remove_uuid_from_metadata,
)
from ch_tools.chadmin.internal.database_replica import (
    ZookeeperDatabaseManager,
    create_database_nodes,
    restore_replica,
)
from ch_tools.chadmin.internal.table import (
    change_table_uuid,
    detach_table,
    list_tables,
    read_local_table_metadata,
)
from ch_tools.chadmin.internal.table_info import TableInfo
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.chadmin.internal.zookeeper import (
    ZKTransactionBuilder,
    delete_zk_node,
    get_zk_node,
    zk_client,
)
from ch_tools.common import logging


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

            with ZKTransactionBuilder(self.ctx, zk) as builder:
                self.zk_manager.create_query_log_entry(
                    builder, prefix_db_zk_path, counter
                )
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

            zk_metadata_path = self.zk_manager.get_default_table_in_db_path(
                database_name, table_name
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


# Public API functions


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
