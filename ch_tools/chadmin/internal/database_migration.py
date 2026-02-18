"""
Database migration utilities for ClickHouse.

Handles migration between Atomic and Replicated database engines,
including ZooKeeper structure management and replica restoration.
"""

from operator import itemgetter
from typing import Any, Literal, Optional

from click import Context

from ch_tools.chadmin.cli import table_metadata_parser
from ch_tools.chadmin.cli.database_metadata import (
    DatabaseEngine,
    parse_database_metadata,
)
from ch_tools.chadmin.cli.server_group import restart_command
from ch_tools.chadmin.internal.database import attach_database, detach_database
from ch_tools.chadmin.internal.database_replica import (
    check_database_exists_in_zk,
    get_replicated_db_table_zk_path,
    get_replicated_db_tables_zk_metadata,
    supports_system_restore_database_replica,
    system_restore_database_replica,
)
from ch_tools.chadmin.internal.schema_comparison import (
    compare_schemas_simple,
    generate_schema_diff,
)
from ch_tools.chadmin.internal.table import list_tables
from ch_tools.chadmin.internal.table_info import TableInfo
from ch_tools.chadmin.internal.table_metadata import (
    TableMetadataManager,
    read_local_table_metadata,
)
from ch_tools.chadmin.internal.zookeeper import (
    delete_zk_node,
    get_zk_node,
)
from ch_tools.common import logging


class AttacherContext:
    """Context manager for database detach/attach operations."""

    def __init__(self, ctx: Context, database: str):
        self.ctx = ctx
        self.database = database

    def __enter__(self) -> None:
        detach_database(self.ctx, self.database)

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_value: Optional[Exception],
        traceback: Optional[Any],
    ) -> Literal[False]:
        attach_database(self.ctx, self.database)
        if exc_type is not None:
            logging.error(
                f"Exception in AttacherContext: {exc_type.__name__}: {exc_value}"
            )
        return False


class DatabaseMigrator:
    """Handles database migration between Atomic and Replicated engines."""

    def __init__(self, ctx: Context):
        self.ctx = ctx

    def migrate_to_atomic(self, database: str, clean_zookeeper: bool) -> None:
        """Migrate Replicated database to Atomic engine with optional ZooKeeper cleanup."""
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

    def migrate_to_replicated(self, database: str) -> None:
        """Migrate Atomic database to Replicated engine with automatic UUID sync and replica restoration."""
        # Validate ClickHouse version supports migration
        if not supports_system_restore_database_replica(self.ctx):
            raise RuntimeError(
                "Migration requires ClickHouse version 25.8 or above. "
                "Current version does not support SYSTEM RESTORE DATABASE REPLICA."
            )

        # Validate database has Atomic engine
        metadata_db = parse_database_metadata(database)
        if metadata_db.database_engine != DatabaseEngine.ATOMIC:
            raise RuntimeError(
                f"Database {database} has engine {metadata_db.database_engine}. "
                "Migration to Replicated from Atomic only is supported."
            )

        first_replica = not check_database_exists_in_zk(
            self.ctx, database, metadata_db.zookeeper_path
        )
        logging.info(
            f"Migrating database {database} as {'first' if first_replica else 'non-first'} replica"
        )

        tables = (
            list_tables(self.ctx, database_name=database) if not first_replica else []
        )

        detach_database(self.ctx, database)
        logging.info(f"Detached database {database}")

        need_restart = False
        if not first_replica:
            self._check_tables_consistent(database, tables)
            need_restart = self._sync_table_uuids(tables)

        metadata_db.set_replicated()
        logging.info(f"Changed {database} engine to Replicated in metadata")

        if need_restart:
            logging.info("Restarting ClickHouse server due to UUID changes")
            self.ctx.invoke(restart_command, timeout=None)
        else:
            attach_database(self.ctx, database)
            logging.info(f"Attached database {database}")

        system_restore_database_replica(self.ctx, database)
        logging.info(f"Successfully migrated database {database} to Replicated")

    def _check_tables_consistent(
        self, database_name: str, local_tables: list[TableInfo]
    ) -> None:
        """Verify local tables match ZooKeeper metadata."""
        zk_tables = get_replicated_db_tables_zk_metadata(self.ctx, database_name)
        missing_in_zk = []
        schema_mismatches = []
        diff_outputs = []

        for table in local_tables:
            if table["name"] not in zk_tables:
                missing_in_zk.append(table["name"])
                continue

            local_metadata = read_local_table_metadata(self.ctx, table["metadata_path"])
            zk_metadata = zk_tables[table["name"]]
            if not compare_schemas_simple(
                local_metadata,
                zk_metadata,
                ignore_uuid=True,
                ignore_engine=False,
                remove_replicated=True,
            ):
                schema_mismatches.append(table["name"])

                # Generate colored diff for better visibility
                diff_output = generate_schema_diff(
                    local_metadata,
                    zk_metadata,
                    f"Local: {table['name']}",
                    f"ZooKeeper: {table['name']}",
                    colored_output=True,
                    ignore_uuid=True,
                    ignore_engine=False,
                    remove_replicated=False,
                )

                diff_outputs.append(
                    f"Table {table['name']}: schema mismatch detected.\n{diff_output}"
                )

        if missing_in_zk or schema_mismatches:
            error_msg = f"Database '{database_name}' tables are inconsistent."
            if missing_in_zk:
                error_msg += f"\nMissing in ZK: {missing_in_zk}."
            if schema_mismatches:
                error_msg += f"\nSchema mismatches: {schema_mismatches}."

            logging.error(error_msg)
            logging.error(
                f"Local tables: {sorted(local_tables, key=itemgetter('name'))}"
            )
            logging.error(f"ZooKeeper tables: {sorted(zk_tables)}")

            for diff_output in diff_outputs:
                logging.error(diff_output)

            raise RuntimeError(error_msg)

    def _sync_table_uuids(self, tables_info: list[TableInfo]) -> bool:
        """Synchronize table UUIDs with ZooKeeper metadata, returns True if any UUID was changed."""
        was_changed = False

        for table in tables_info:
            table_name = table["name"]
            database_name = table["database"]
            old_table_uuid = table["uuid"]

            zk_metadata_path = get_replicated_db_table_zk_path(
                database_name, table_name
            )
            zk_table_metadata = get_zk_node(self.ctx, zk_metadata_path)
            zk_table_uuid = table_metadata_parser.parse_uuid(zk_table_metadata)

            if zk_table_uuid == old_table_uuid:
                logging.debug(f"Table {database_name}.{table_name}: UUID unchanged")
                continue

            logging.info(
                f"Updating UUID for {database_name}.{table_name}: {old_table_uuid} -> {zk_table_uuid}"
            )
            was_changed = True

            TableMetadataManager.change_uuid(
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


def migrate_database_to_atomic(
    ctx: Context, database: str, clean_zookeeper: bool
) -> None:
    """Migrate Replicated database to Atomic engine."""
    migrator = DatabaseMigrator(ctx)
    migrator.migrate_to_atomic(database, clean_zookeeper)


def migrate_database_to_replicated(ctx: Context, database: str) -> None:
    """Migrate Atomic database to Replicated engine."""
    migrator = DatabaseMigrator(ctx)
    migrator.migrate_to_replicated(database)
