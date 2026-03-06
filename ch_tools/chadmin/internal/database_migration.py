"""
Database migration utilities for ClickHouse.

Handles migration between Atomic and Replicated database engines,
including ZooKeeper structure management and replica restoration.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from click import Context

from ch_tools.chadmin.cli.database_metadata import (
    DatabaseEngine,
    parse_database_metadata,
)
from ch_tools.chadmin.cli.server_group import restart_command
from ch_tools.chadmin.internal.database import (
    attach_database,
    detach_database,
    is_database_exists,
)
from ch_tools.chadmin.internal.database_replica import (
    ZK_REPLICAS_SUBPATH,
    DatabaseLockManager,
    _get_database_zk_path,
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
from ch_tools.chadmin.internal.table_metadata_manager import (
    TableMetadataManager,
    read_local_table_metadata,
)
from ch_tools.chadmin.internal.table_metadata_parser import parse_uuid
from ch_tools.chadmin.internal.zookeeper import (
    check_zk_node,
    delete_zk_node,
    get_zk_node,
)
from ch_tools.common import logging
from ch_tools.common.clickhouse.config import get_macros


class CheckStatus(Enum):
    """Status of individual check execution."""

    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"
    SKIPPED = "SKIPPED"


class MigrationDirection(Enum):
    TO_REPLICATED = "atomic_to_replicated"
    TO_ATOMIC = "replicated_to_atomic"


@dataclass
class CheckResult:
    """
    Result of executing a check.
    """

    check_name: str
    status: CheckStatus
    message: str
    details: Optional[Dict[str, Any]] = None
    requires_restart: bool = False


@dataclass
class MigrationCheckReport:
    """
    Final report on pre-migration checks.
    """

    database: str
    can_migrate: bool
    requires_restart: bool
    checks: List[CheckResult]
    errors: List[str]
    warnings: List[str]
    direction: Optional[MigrationDirection] = None

    def raise_if_failed(self) -> None:
        """Raise an exception if migration checks failed."""
        if not self.can_migrate:
            raise MigrationCheckFailedError(self)


# Expected digest value for a clean replica
EXPECTED_DIGEST_VALUE = "0"


class MigrationError(Exception):
    """Base exception for migration errors."""

    pass


class MigrationCheckFailedError(MigrationError):
    """Raised when pre-migration checks fail."""

    def __init__(self, report: MigrationCheckReport):
        self.report = report
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        return "Migration checks failed:\n" + "\n".join(
            f"  - {e}" for e in self.report.errors
        )


class AttacherContext:
    """Context manager for database detach/attach operations with optional restart support."""

    def __init__(self, ctx: Context, database: str):
        self.ctx = ctx
        self.database = database
        self._restart_requested = False

    def __enter__(self) -> "AttacherContext":
        detach_database(self.ctx, self.database)
        logging.info(f"Detached database {self.database}")
        return self

    def request_restart(self) -> None:
        self._restart_requested = True
        logging.info("Restart requested instead of attach")

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_value: Optional[Exception],
        traceback: Optional[Any],
    ) -> Literal[False]:
        if exc_type is not None:
            logging.error(
                f"Exception in AttacherContext: {exc_type.__name__}: {exc_value}"
            )
            self._try_attach()
            return False

        if self._restart_requested:
            logging.info("Restarting ClickHouse server due to UUID changes")
            self.ctx.invoke(restart_command, timeout=None)
        else:
            self._try_attach()

        return False

    def _try_attach(self) -> None:
        try:
            attach_database(self.ctx, self.database)
            logging.info(f"Attached database {self.database}")
        except Exception as e:
            logging.debug(f"Attach failed (may be normal after restart): {e}")


class DatabaseMigrator:  # pylint: disable=too-many-instance-attributes
    """Handles database migration between Atomic and Replicated engines."""

    def __init__(self, ctx: Context, database: str, clean_zookeeper: bool = False):
        if not database:
            raise ValueError("Database name is required")

        self.ctx = ctx
        self.database = database
        self._clean_zookeeper = clean_zookeeper

        # Check database exists before parsing metadata
        if not is_database_exists(self.ctx, self.database):
            raise MigrationError(
                f"Database {self.database} does not exists, skip migrating"
            )

        self.metadata_db = parse_database_metadata(self.database)

        if self.metadata_db.database_engine == DatabaseEngine.ATOMIC:
            self.direction = MigrationDirection.TO_REPLICATED
        else:
            self.direction = MigrationDirection.TO_ATOMIC

        self._db_zk_path = _get_database_zk_path(
            self.database, self.metadata_db.zookeeper_path
        )
        macros = get_macros(self.ctx)
        self.shard = macros.get("shard")
        self.replica = macros.get("replica")
        self._replica_path = (
            f"{self._db_zk_path}/{ZK_REPLICAS_SUBPATH}/{self.shard}|{self.replica}"
        )
        self._digest_path = f"{self._replica_path}/digest"
        self.local_tables: list = []

    def _run_pre_migration_checks(
        self, in_action: bool = False, first_replica: bool = True
    ) -> None:
        if self.direction == MigrationDirection.TO_REPLICATED:
            self._check_source_database_state()
            # Load tables for UUID sync and consistency check on non-first replica
            if in_action and not first_replica:
                self.local_tables = list_tables(self.ctx, database_name=self.database)
                self._check_tables_consistency()
        else:
            # TO_ATOMIC migration
            self.local_tables = list_tables(self.ctx, database_name=self.database)
            if not in_action:
                self._check_source_database_state()
                self._check_clickhouse_version()
                self._sync_table_uuids(dry_run=True)
                self._check_replica_digest()
                self._check_tables_consistency()

    def _check_source_database_state(self) -> None:
        expected_engine = (
            DatabaseEngine.ATOMIC
            if self.direction == MigrationDirection.TO_REPLICATED
            else DatabaseEngine.REPLICATED
        )
        if self.metadata_db.database_engine != expected_engine:
            raise MigrationError(
                f"Database engine is {self.metadata_db.database_engine}, expected {expected_engine} for {self.direction.value} migration"
            )

    def _check_clickhouse_version(self) -> None:
        if not supports_system_restore_database_replica(self.ctx):
            raise MigrationError(
                "ClickHouse version does not support SYSTEM RESTORE DATABASE REPLICA"
            )

    def _check_replica_digest(self) -> None:
        if not check_zk_node(self.ctx, self._digest_path):
            return
        digest_value = get_zk_node(self.ctx, self._digest_path)
        if digest_value != EXPECTED_DIGEST_VALUE:
            # For TO_ATOMIC migration, non-zero digest is just a warning
            # For TO_REPLICATED, it's an error
            if self.direction == MigrationDirection.TO_ATOMIC:
                logging.warning(
                    f"Replica digest is {digest_value}, expected {EXPECTED_DIGEST_VALUE}. "
                    "Database may have uncommitted changes, but migration to Atomic will proceed."
                )
            else:
                raise MigrationError(
                    f"Replica digest is {digest_value}, expected {EXPECTED_DIGEST_VALUE}. Database has uncommitted changes."
                )

    def _check_tables_consistency(self) -> None:
        """Verify local tables match ZooKeeper metadata."""
        zk_tables = get_replicated_db_tables_zk_metadata(
            self.ctx, self.database, self.metadata_db.zookeeper_path
        )

        missing_in_zk = []
        schema_mismatches = []
        diff_outputs = []

        for table in self.local_tables:
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
            error_msg = f"Database '{self.database}' tables are inconsistent."
            if missing_in_zk:
                error_msg += f"\nMissing in ZK: {missing_in_zk}."
            if schema_mismatches:
                error_msg += f"\nSchema mismatches: {schema_mismatches}."

            logging.error(error_msg)
            logging.error(
                f"Local tables: {sorted(self.local_tables, key=lambda x: x['name'])}"
            )
            logging.error(f"ZooKeeper tables: {sorted(zk_tables)}")

            for diff_output in diff_outputs:
                logging.error(diff_output)

            raise RuntimeError(error_msg)

    def migrate_to_atomic(
        self, database: str, clean_zookeeper: bool, dry_run: bool = False
    ) -> bool:
        if self.metadata_db.database_engine != DatabaseEngine.REPLICATED:
            raise RuntimeError(
                f"Database {database} has engine {self.metadata_db.database_engine}. "
                "Migration to Atomic from Replicated only is supported."
            )

        logging.info("Running pre-migration checks for migration to Atomic...")
        self._run_pre_migration_checks()

        if dry_run:
            logging.info("Dry-run mode: checks completed: OK")
            return True

        with AttacherContext(self.ctx, database):
            zookeeper_path = self.metadata_db.zookeeper_path
            self.metadata_db.set_atomic()

            if clean_zookeeper and zookeeper_path:
                try:
                    logging.info(f"Cleaning ZooKeeper nodes: {zookeeper_path}")
                    delete_zk_node(self.ctx, zookeeper_path)
                except Exception as e:
                    logging.warning(
                        f"Failed to clean ZooKeeper nodes at {zookeeper_path}: {e}"
                    )

        logging.info(f"Database {database} migrated to Atomic")
        return True

    def migrate_to_replicated(
        self,
        database: str,
        force_remove_lock: bool = False,
        dry_run: bool = False,
    ) -> bool:
        if self.metadata_db.database_engine != DatabaseEngine.ATOMIC:
            raise RuntimeError(
                f"Database {database} has engine {self.metadata_db.database_engine}. "
                "Migration to Replicated from Atomic only is supported."
            )

        logging.info("Running pre-migration checks for migration to Replicated...")
        self._run_pre_migration_checks()

        if dry_run:
            logging.info("Dry-run mode: checks completed: OK")
            return True

        # Execute the actual migration
        with DatabaseLockManager(
            self.ctx, database, self.metadata_db.zookeeper_path, force_remove_lock
        ) as first_replica:
            self._run_pre_migration_checks(in_action=True, first_replica=first_replica)

            with AttacherContext(self.ctx, database) as attacher:
                self.metadata_db.set_replicated()
                logging.info(f"Changed {database} engine to Replicated in metadata")

                if not first_replica and self._sync_table_uuids(dry_run=False):
                    attacher.request_restart()

            system_restore_database_replica(self.ctx, database)
            logging.info(f"Successfully migrated database {database} to Replicated")
        return True

    def _sync_table_uuids(self, dry_run: bool) -> bool:
        """Synchronize table UUIDs with ZooKeeper metadata, returns True if any UUID was changed."""
        was_changed = False

        for table in self.local_tables:
            table_name = table["name"]
            database_name = table["database"]
            old_table_uuid = table["uuid"]

            zk_metadata_path = get_replicated_db_table_zk_path(
                database_name, table_name, self.metadata_db.zookeeper_path
            )
            zk_table_metadata = get_zk_node(self.ctx, zk_metadata_path)
            zk_table_uuid = parse_uuid(zk_table_metadata)

            if zk_table_uuid == old_table_uuid:
                logging.debug(f"Table {database_name}.{table_name}: UUID unchanged")
                continue

            logging.info(
                f"Updating UUID for {database_name}.{table_name}: {old_table_uuid} -> {zk_table_uuid}"
            )

            if dry_run:
                logging.info("Dry-run mode: Will need restart")
                return True

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
    ctx: Context, database: str, clean_zookeeper: bool, dry_run: bool = False
) -> bool:
    """Migrate Replicated database to Atomic engine."""
    migrator = DatabaseMigrator(ctx, database, clean_zookeeper)
    return migrator.migrate_to_atomic(database, clean_zookeeper, dry_run)


def migrate_database_to_replicated(
    ctx: Context,
    database: str,
    force_remove_lock: bool = False,
    dry_run: bool = False,
) -> bool:
    """Migrate Atomic database to Replicated engine."""
    migrator = DatabaseMigrator(ctx, database, clean_zookeeper=False)
    return migrator.migrate_to_replicated(database, force_remove_lock, dry_run)
