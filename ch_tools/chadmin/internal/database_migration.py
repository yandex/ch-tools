"""
Database migration utilities for ClickHouse.

Handles migration between Atomic and Replicated database engines,
including ZooKeeper structure management and replica restoration.
"""

import logging
from dataclasses import dataclass
from enum import Enum
from operator import itemgetter
from typing import Any, Dict, List, Literal, Optional

from click import Context, echo, style

from ch_tools.chadmin.cli.database_metadata import (
    DatabaseEngine,
    parse_database_metadata,
)
from ch_tools.chadmin.cli.server_group import restart_command
from ch_tools.chadmin.internal.database import attach_database, detach_database
from ch_tools.chadmin.internal.database_replica import (
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
from ch_tools.chadmin.internal.table_info import TableInfo
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

    def print_summary(self) -> None:
        """Print a formatted summary of the migration check report."""
        # Print header
        echo(style(f"Migration Check Report for database '{self.database}'", fg="cyan", bold=True))
        
        # Print direction if available
        if self.direction:
            echo(f"Migration direction: {self.direction.value}")
        
        # Print overall status
        if self.can_migrate:
            echo(style("✓ Migration checks passed", fg="green"))
        else:
            echo(style("✗ Migration checks failed", fg="red"))
            
        # Print restart requirement
        if self.requires_restart:
            echo(style("⚠ Server restart required after migration", fg="yellow"))
        
        # Print errors if any
        if self.errors:
            echo(style("\nErrors:", fg="red", bold=True))
            for error in self.errors:
                echo(f"  - {error}")
        
        # Print warnings if any
        if self.warnings:
            echo(style("\nWarnings:", fg="yellow", bold=True))
            for warning in self.warnings:
                echo(f"  - {warning}")
        
        # Print individual check results
        echo(style("\nCheck Details:", fg="blue", bold=True))
        for check in self.checks:
            if check.status == CheckStatus.PASSED:
                status_icon = style("✓", fg="green")
            elif check.status == CheckStatus.FAILED:
                status_icon = style("✗", fg="red")
            elif check.status == CheckStatus.WARNING:
                status_icon = style("⚠", fg="yellow")
            else:  # SKIPPED
                status_icon = style("-", fg="white")
                
            echo(f"  {status_icon} {check.check_name}: {check.message}")

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
        return "Migration checks failed:\n" + "\n".join(f"  - {e}" for e in self.report.errors)


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


class DatabaseMigrator:
    """Handles database migration between Atomic and Replicated engines."""

    def __init__(
        self, ctx: Context, database: str = None, db_replica_path: Optional[str] = None
    ):
        self.ctx = ctx
        self.database = database
        self.db_replica_path = db_replica_path
        self.checks_results: List[CheckResult] = []

        # Pre-calculate common paths if database is provided
        if database:
            self.db_replica_path = db_replica_path or self._get_default_zk_path()
            self._db_zk_path = _get_database_zk_path(self.database, self.db_replica_path)
            self._shard: Optional[str] = None
            self._replica: Optional[str] = None
            self._replica_path: Optional[str] = None
            self._digest_path: Optional[str] = None

    def _get_shard_and_replica(self) -> tuple[Optional[str], Optional[str]]:
        """Get shard and replica from macros."""
        macros = get_macros(self.ctx)
        shard = macros.get("shard")
        replica = macros.get("replica")
        return shard, replica

    def _initialize_replica_paths(self) -> bool:
        """Initialize replica-specific paths. Returns False if macros not defined."""
        # More idiomatic way to check for None
        return self._replica_path is not None or self._try_initialize_replica_paths()

    def _try_initialize_replica_paths(self) -> bool:
        """Try to initialize replica paths from macros."""
        shard, replica = self._get_shard_and_replica()
        if not shard or not replica:
            return False

        self._shard = shard
        self._replica = replica
        self._replica_path = f"{self._db_zk_path}/replicas/{shard}|{replica}"
        self._digest_path = f"{self._replica_path}/digest"
        return True

    def _get_default_zk_path(self) -> str:
        """Get default ZooKeeper path for database."""
        return f"/clickhouse/{self.database}"

    def run_pre_migration_checks(self, direction: MigrationDirection, clean_zookeeper: bool = False) -> MigrationCheckReport:
        """
        Execute all checks and return result.
        """
        checks = []
        errors = []
        warnings = []
        requires_restart = False

        # Execute only essential checks
        check_methods = [
            self._check_replica_digest,
            self._check_tables_consistency,
        ]
        
        # Add database state check with proper expected engine
        def _check_database_state_for_direction():
            expected_engine = (
                DatabaseEngine.REPLICATED 
                if direction == MigrationDirection.TO_REPLICATED 
                else DatabaseEngine.ATOMIC
            )
            return self._check_source_database_state(expected_engine)
        
        check_methods.insert(1, _check_database_state_for_direction)

        # Add ZooKeeper cleanup check if needed
        if clean_zookeeper and direction == MigrationDirection.TO_ATOMIC:
            check_methods.append(self._check_safe_to_remove_zk_nodes)

        for check_method in check_methods:
            try:
                # Pass clean_zookeeper parameter to the check method if it accepts it
                import inspect
                sig = inspect.signature(check_method)
                if 'clean_zookeeper' in sig.parameters:
                    result = check_method(clean_zookeeper)
                else:
                    result = check_method()
                checks.append(result)

                if result.status == CheckStatus.FAILED:
                    errors.append(f"{result.check_name}: {result.message}")
                elif result.status == CheckStatus.WARNING:
                    warnings.append(f"{result.check_name}: {result.message}")

                if result.requires_restart:
                    requires_restart = True

            except Exception as e:
                logging.error(
                    "Check %s failed with exception: %s",
                    check_method.__name__,
                    e,
                )
                checks.append(
                    CheckResult(
                        check_name=check_method.__name__,
                        status=CheckStatus.FAILED,
                        message=f"Check failed with exception: {e}",
                    )
                )
                errors.append(f"{check_method.__name__}: {e}")

        # Check if restart is required for UUID synchronization
        if not requires_restart and direction == MigrationDirection.TO_REPLICATED:
            # For migration to replicated, check if UUID sync will be needed
            requires_restart = self._will_require_restart()

        can_migrate = len(errors) == 0

        return MigrationCheckReport(
            database=self.database,
            can_migrate=can_migrate,
            requires_restart=requires_restart,
            checks=checks,
            errors=errors,
            warnings=warnings,
            direction=direction,
        )

    def _check_safe_to_remove_zk_nodes(self, clean_zookeeper: bool) -> CheckResult:
        """
        Check if it's safe to remove ZooKeeper nodes during migration to Atomic.
        
        This check ensures that:
        1. ZooKeeper path is defined for the database
        2. If clean_zookeeper is True, the path exists and can be safely removed
        3. If clean_zookeeper is False, no action is needed
        """
        try:
            # If not cleaning ZooKeeper, this check is not applicable
            if not clean_zookeeper:
                return CheckResult(
                    check_name="ZooKeeper Cleanup Safety",
                    status=CheckStatus.SKIPPED,
                    message="ZooKeeper cleanup not requested",
                )

            # Parse database metadata to get ZooKeeper path
            metadata_db = parse_database_metadata(self.database)
            
            # Check if database has a ZooKeeper path defined
            if not metadata_db.zookeeper_path:
                return CheckResult(
                    check_name="ZooKeeper Cleanup Safety",
                    status=CheckStatus.FAILED,
                    message="Database does not have a ZooKeeper path defined",
                )

            zk_path = metadata_db.zookeeper_path
            
            # Check if the ZooKeeper path exists
            zk_stat = check_zk_node(self.ctx, zk_path)
            if not zk_stat:
                return CheckResult(
                    check_name="ZooKeeper Cleanup Safety",
                    status=CheckStatus.WARNING,
                    message=f"ZooKeeper path '{zk_path}' does not exist (already cleaned or never existed)",
                )

            # If path exists, it's safe to remove (no additional validation needed)
            return CheckResult(
                check_name="ZooKeeper Cleanup Safety",
                status=CheckStatus.PASSED,
                message=f"ZooKeeper path '{zk_path}' exists and can be safely removed",
                details={
                    "zookeeper_path": zk_path,
                    "creation_time": zk_stat.creation_time if hasattr(zk_stat, 'creation_time') else None,
                }
            )

        except Exception as e:
            return CheckResult(
                check_name="ZooKeeper Cleanup Safety",
                status=CheckStatus.WARNING,
                message=f"Could not check ZooKeeper cleanup safety: {e}",
            )

    def _check_replica_digest(self) -> CheckResult:
        """
        Check 3: Replica digest value (CRITICAL).
        """
        try:
            # Initialize replica paths
            if not self._initialize_replica_paths():
                return CheckResult(
                    check_name="Replica Digest Check",
                    status=CheckStatus.SKIPPED,
                    message="Macros 'shard' or 'replica' not defined",
                )

            # Check if digest node exists
            if self._digest_path is None:
                return CheckResult(
                    check_name="Replica Digest Check",
                    status=CheckStatus.FAILED,
                    message="Digest path is not initialized",
                )
                
            digest_stat = check_zk_node(self.ctx, self._digest_path)
            if not digest_stat:
                return CheckResult(
                    check_name="Replica Digest Check",
                    status=CheckStatus.PASSED,
                    message="Replica digest node does not exist (OK for new replica)",
                )

            # Get digest value
            digest_value = get_zk_node(self.ctx, self._digest_path)

            if digest_value != EXPECTED_DIGEST_VALUE:
                return CheckResult(
                    check_name="Replica Digest Check",
                    status=CheckStatus.FAILED,
                    message=f"Replica digest is '{digest_value}', expected '{EXPECTED_DIGEST_VALUE}'. "
                    f"This will cause REPLICA_ALREADY_EXISTS error. "
                    f"Run: SYSTEM DROP REPLICA '{self._shard}|{self._replica}' FROM DATABASE {self.database}",
                    details={
                        "digest": digest_value,
                        "replica_path": self._replica_path,
                    },
                )

            return CheckResult(
                check_name="Replica Digest Check",
                status=CheckStatus.PASSED,
                message="Replica digest is '0' (OK)",
            )

        except (Exception,) as e:
            return CheckResult(
                check_name="Replica Digest Check",
                status=CheckStatus.WARNING,
                message=f"Could not check replica digest: {e}",
            )

    def _check_source_database_state(self, expected_engine: DatabaseEngine) -> CheckResult:
        """
        Check 5: Database state.
        """
        try:
            # Parse database metadata
            metadata_db = parse_database_metadata(self.database)

            # Check database engine
            if metadata_db.database_engine != expected_engine:
                return CheckResult(
                    check_name="Database State",
                    status=CheckStatus.FAILED,
                    message=f"Database has engine {metadata_db.database_engine}, expected {expected_engine}",
                )

            return CheckResult(
                check_name="Database State",
                status=CheckStatus.PASSED,
                message=f"Database is {metadata_db.database_engine}, ready for migration",
            )

        except Exception as e:
            return CheckResult(
                check_name="Database State",
                status=CheckStatus.FAILED,
                message=f"Could not check database state: {e}",
            )

    def _check_tables_consistency(self) -> CheckResult:
        """
        Check 8-9: Consistency of UUIDs and table schemas.
        """
        try:
            # Get local tables
            local_tables = list_tables(self.ctx, database_name=self.database)

            if not local_tables:
                return CheckResult(
                    check_name="Tables Consistency",
                    status=CheckStatus.PASSED,
                    message="No tables to check for consistency",
                )

            # Get tables from ZooKeeper
            zk_tables = get_replicated_db_tables_zk_metadata(
                self.ctx, self.database, self.db_replica_path
            )

            missing_in_zk = []
            schema_mismatches = []
            diff_outputs = []

            for table in local_tables:
                table_name = table["name"]

                # Check if table exists in ZK
                if table_name not in zk_tables:
                    missing_in_zk.append(table_name)
                    continue

                # Get local metadata
                local_metadata = read_local_table_metadata(self.ctx, table["metadata_path"])
                zk_metadata = zk_tables[table_name]

                # Check schema consistency
                if not compare_schemas_simple(
                    local_metadata,
                    zk_metadata,
                    ignore_uuid=True,
                    ignore_engine=False,
                    remove_replicated=True,
                    collapse_whitespace=True,
                ):
                    schema_mismatches.append(table_name)

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

            # Prepare result
            issues = []
            if missing_in_zk:
                issues.append(f"Missing in ZK: {missing_in_zk}")
            if schema_mismatches:
                issues.append(f"Schema mismatches: {schema_mismatches}")

            if issues:
                error_msg = f"Database '{self.database}' tables are inconsistent."
                if missing_in_zk:
                    error_msg += f"\nMissing in ZK: {missing_in_zk}."
                if schema_mismatches:
                    error_msg += f"\nSchema mismatches: {schema_mismatches}."

                details = {
                    "missing_in_zk": missing_in_zk,
                    "schema_mismatches": schema_mismatches,
                }

                # Include detailed diff outputs if available
                if diff_outputs:
                    details["detailed_diffs"] = diff_outputs

                return CheckResult(
                    check_name="Tables Consistency",
                    status=CheckStatus.FAILED,
                    message=error_msg,
                    details=details,
                )

            return CheckResult(
                check_name="Tables Consistency",
                status=CheckStatus.PASSED,
                message=f"All {len(local_tables)} tables are consistent with ZooKeeper",
            )

        except (Exception,) as e:
            return CheckResult(
                check_name="Tables Consistency",
                status=CheckStatus.WARNING,
                message=f"Could not check tables consistency: {e}",
            )

    def _will_require_restart(self) -> bool:
        """
        Determine if restart is required for UUID synchronization.
        """
        try:
            # Get local tables
            tables = list_tables(self.ctx, database_name=self.database)
            
            if not tables:
                return False

            # For each table, check if UUID matches ZooKeeper
            uuid_mismatch_found = False
            for table in tables:
                table_name = table["name"]
                old_table_uuid = table["uuid"]

                zk_metadata_path = get_replicated_db_table_zk_path(
                    self.database, table_name, self.db_replica_path
                )

                try:
                    zk_table_metadata = get_zk_node(self.ctx, zk_metadata_path)
                    zk_table_uuid = parse_uuid(zk_table_metadata)

                    if zk_table_uuid != old_table_uuid:
                        logging.info(
                            "Table %s.%s UUID mismatch: local=%s, zk=%s",
                            self.database,
                            table_name,
                            old_table_uuid,
                            zk_table_uuid,
                        )
                        uuid_mismatch_found = True
                        # Don't return immediately to log all mismatches
                except Exception as e:
                    # Log the error but don't fail the check
                    logging.debug(
                        "Could not get UUID for table %s.%s from ZooKeeper: %s",
                        self.database,
                        table_name,
                        e,
                    )
                    # Continue with other tables

            if uuid_mismatch_found:
                logging.info("UUID synchronization will require server restart")
                return True

            return False

        except (Exception,) as e:
            logging.warning("Could not determine if restart is required: %s", e)
            # In case of error, we should be conservative and not require restart
            # since we couldn't determine the state properly
            return False

    def migrate_to_atomic(
        self, database: str, clean_zookeeper: bool, dry_run: bool = False
    ) -> Optional[MigrationCheckReport]:
        """Migrate Replicated database to Atomic engine with optional ZooKeeper cleanup."""
        # Run pre-migration checks
        self.__init__(self.ctx, database)
        logging.info("Running pre-migration checks for migration to Atomic...")
        check_report = self.run_pre_migration_checks(MigrationDirection.TO_ATOMIC, clean_zookeeper)
        
        if not check_report.can_migrate:
            logging.error("Pre-migration checks failed:")
            for error in check_report.errors:
                logging.error(f"  - {error}")
            raise RuntimeError("Cannot proceed with migration due to failed checks")
            
        if dry_run:
            logging.info("Dry-run mode: checks completed successfully")
            logging.info(
                f"Migration is possible. Restart required: {check_report.requires_restart}"
            )
            return check_report
            
        # Execute the actual migration
        return self._execute_migration_to_atomic(database, clean_zookeeper)

    def _execute_migration_to_atomic(self, database: str, clean_zookeeper: bool) -> Optional[MigrationCheckReport]:
        """Execute the actual migration from Replicated to Atomic engine."""
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
        return None

    def migrate_to_replicated(
        self,
        database: str,
        force_remove_lock: bool = False,
        dry_run: bool = False,
    ) -> Optional[MigrationCheckReport]:
        """Migrate Atomic database to Replicated engine."""
        if not supports_system_restore_database_replica(self.ctx):
            raise RuntimeError(
                "Migration requires ClickHouse version 25.8 or above. "
                "Current version does not support SYSTEM RESTORE DATABASE REPLICA."
            )

        metadata_db = parse_database_metadata(database)
        if metadata_db.database_engine != DatabaseEngine.ATOMIC:
            raise RuntimeError(
                f"Database {database} has engine {metadata_db.database_engine}. "
                "Migration to Replicated from Atomic only is supported."
            )

        # Run pre-migration checks
        self.__init__(self.ctx, database, metadata_db.zookeeper_path)
        logging.info("Running pre-migration checks for migration to Replicated...")
        check_report = self.run_pre_migration_checks(MigrationDirection.TO_REPLICATED, clean_zookeeper=False)

        if not check_report.can_migrate:
            logging.error("Pre-migration checks failed:")
            for error in check_report.errors:
                logging.error(f"  - {error}")
            raise RuntimeError("Cannot proceed with migration due to failed checks")

        if dry_run:
            logging.info("Dry-run mode: checks completed successfully")
            logging.info(
                f"Migration is possible. Restart required: {check_report.requires_restart}"
            )
            return check_report

        # Execute the actual migration
        return self._execute_migration_to_replicated(database, force_remove_lock, check_report)

    def _execute_migration_to_replicated(
        self,
        database: str,
        force_remove_lock: bool,
        check_report: MigrationCheckReport,
    ) -> Optional[MigrationCheckReport]:
        """Execute the actual migration from Atomic to Replicated engine."""
        metadata_db = parse_database_metadata(database)
        
        with DatabaseLockManager(
            self.ctx, database, metadata_db.zookeeper_path, force_remove_lock
        ) as first_replica:
            tables = []
            if not first_replica:
                tables = list_tables(self.ctx, database_name=database)
                # Tables consistency already checked in run_pre_migration_checks()

            with AttacherContext(self.ctx, database) as attacher:
                metadata_db.set_replicated()
                logging.info(f"Changed {database} engine to Replicated in metadata")

                if not first_replica and self._sync_table_uuids(tables):
                    attacher.request_restart()

            system_restore_database_replica(self.ctx, database)
            logging.info(f"Successfully migrated database {database} to Replicated")

        return check_report

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
            zk_table_uuid = parse_uuid(zk_table_metadata)

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
    ctx: Context, database: str, clean_zookeeper: bool, dry_run: bool = False
) -> Optional[MigrationCheckReport]:
    """Migrate Replicated database to Atomic engine."""
    migrator = DatabaseMigrator(ctx)
    return migrator.migrate_to_atomic(database, clean_zookeeper, dry_run)


def migrate_database_to_replicated(
    ctx: Context,
    database: str,
    force_remove_lock: bool = False,
    dry_run: bool = False,
) -> Optional[MigrationCheckReport]:
    """Migrate Atomic database to Replicated engine."""
    migrator = DatabaseMigrator(ctx)
    return migrator.migrate_to_replicated(database, force_remove_lock, dry_run)
