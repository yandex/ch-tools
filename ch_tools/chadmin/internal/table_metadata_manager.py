"""Table metadata manager utilities."""

import grp
import os
import pwd
import re
from typing import TYPE_CHECKING

from click import ClickException

from ch_tools.chadmin.internal.clickhouse_disks import CLICKHOUSE_PATH
from ch_tools.chadmin.internal.system import match_ch_version
from ch_tools.chadmin.internal.table import detach_table
from ch_tools.chadmin.internal.table_metadata_parser import (
    UUID_PATTERN,
    MetadataFileError,
    TableMetadataParser,
)
from ch_tools.chadmin.internal.zookeeper import get_table_shared_id
from ch_tools.common import logging

if TYPE_CHECKING:
    from click import Context


class TableMetadataManager:
    """Metadata file operations manager."""

    @staticmethod
    def _verify_possible_change_uuid(
        ctx: "Context", table_local_metadata_path: str, dst_uuid: str
    ) -> None:
        """Verify that UUID change is possible for the table."""
        logging.debug(
            "call _verify_possible_change_uuid with path={}, new uuid={}",
            table_local_metadata_path,
            dst_uuid,
        )
        metadata = TableMetadataParser.parse(table_local_metadata_path)

        if not metadata.table_engine.is_table_engine_replicated():
            return

        assert metadata.replica_path is not None

        logging.debug(
            "Table metadata={} with Replicated table engine, replica_name={}, replica_path={}",
            table_local_metadata_path,
            metadata.replica_name,
            metadata.replica_path,
        )
        if "{uuid}" in metadata.replica_path:
            raise ClickException(
                f"Changing uuid for ReplicatedMergeTree that contains macros uuid in replica path was not allowed. replica_path={metadata.replica_path}"
            )

        table_shared_id = get_table_shared_id(ctx, metadata.replica_path)

        logging.debug(
            "Check that dst_uuid {} is equal with table_shared_id {} node.",
            dst_uuid,
            table_shared_id,
        )

        if dst_uuid != table_shared_id:
            logging.warning(
                f"dst_uuid={dst_uuid} is different from table_shared_id={table_shared_id}."
            )

    @staticmethod
    def update_uuid(table_local_metadata_path: str, new_uuid: str) -> None:
        """Update UUID in metadata file."""
        logging.debug(
            "Updating UUID to {} in metadata file {}",
            new_uuid,
            table_local_metadata_path,
        )

        if not os.path.exists(table_local_metadata_path):
            raise MetadataFileError(
                f"Metadata file not found: '{table_local_metadata_path}'"
            )

        try:
            with open(table_local_metadata_path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            if not lines or len(lines[0]) == 0:
                raise MetadataFileError(
                    f"Metadata file is empty: '{table_local_metadata_path}'"
                )

            lines[0] = re.sub(UUID_PATTERN, f"UUID '{new_uuid}'", lines[0])

            with open(table_local_metadata_path, "w", encoding="utf-8") as f:
                f.writelines(lines)

            logging.debug("UUID successfully updated in metadata file")
        except OSError as e:
            raise MetadataFileError(
                f"Failed to update UUID in metadata file '{table_local_metadata_path}': {e}"
            ) from e

    @staticmethod
    def get_table_store_path(table_uuid: str) -> str:
        """Get table storage path by UUID."""
        return f"{CLICKHOUSE_PATH}/store/{table_uuid[:3]}/{table_uuid}"

    @staticmethod
    def move_table_store(old_table_uuid: str, new_uuid: str) -> None:
        """Move table storage to new UUID."""
        logging.debug("Moving table store from UUID {} to {}", old_table_uuid, new_uuid)

        old_table_store_path = TableMetadataManager.get_table_store_path(old_table_uuid)
        logging.debug("Old table store path: {}", old_table_store_path)

        if not os.path.exists(old_table_store_path):
            raise MetadataFileError(
                f"Table store directory not found: '{old_table_store_path}'"
            )

        target_dir = f"{CLICKHOUSE_PATH}/store/{new_uuid[:3]}"

        if not os.path.exists(target_dir):
            logging.debug("Creating target directory: {}", target_dir)
            try:
                os.mkdir(target_dir)
                os.chmod(target_dir, 0o750)
                uid = pwd.getpwnam("clickhouse").pw_uid
                gid = grp.getgrnam("clickhouse").gr_gid
                os.chown(target_dir, uid, gid)
            except OSError as e:
                raise MetadataFileError(
                    f"Failed to create target directory '{target_dir}': {e}"
                ) from e
        else:
            logging.debug("Target directory already exists: {}", target_dir)

        new_table_store_path = TableMetadataManager.get_table_store_path(new_uuid)
        logging.debug("New table store path: {}", new_table_store_path)

        try:
            os.rename(old_table_store_path, new_table_store_path)

            uid = pwd.getpwnam("clickhouse").pw_uid
            gid = grp.getgrnam("clickhouse").gr_gid
            os.chown(new_table_store_path, uid, gid)

            logging.debug("Table store successfully moved")
        except OSError as e:
            raise MetadataFileError(
                f"Failed to move table store from '{old_table_store_path}' to '{new_table_store_path}': {e}"
            ) from e

    @staticmethod
    def change_uuid(
        ctx: "Context",
        database: str,
        table: str,
        engine: str,
        new_local_uuid: str,
        old_table_uuid: str,
        table_local_metadata_path: str,
        attached: bool,
    ) -> None:
        """Change table UUID in metadata and move table store."""
        logging.debug("call change_uuid with table={}", table)
        if match_ch_version(ctx, "25.1"):
            table_local_metadata_path = f"{CLICKHOUSE_PATH}/{table_local_metadata_path}"

        # Check if engine is a table (not a view)
        is_table_engine = engine not in ["View", "MaterializedView"]
        is_view_engine = engine == "View"

        if is_table_engine:
            logging.debug("{}.{} is a table.", database, table)
            TableMetadataManager._verify_possible_change_uuid(
                ctx, table_local_metadata_path, new_local_uuid
            )
            if old_table_uuid == new_local_uuid:
                logging.info(
                    "Table {}.{} has uuid {}. Don't need to update current table uuid {}. Finish changing",
                    database,
                    table,
                    old_table_uuid,
                    new_local_uuid,
                )
                return

            logging.info(
                "Table's {}.{} uuid {} will be updated to uuid {}",
                database,
                table,
                old_table_uuid,
                new_local_uuid,
            )
        else:
            logging.info("{}.{} is not a table, skip checking.", database, table)

        if attached and not is_view_engine:

            # we could not just detach view - problem with cleanupDetachedTables
            detach_table(
                ctx, database_name=database, table_name=table, permanently=False
            )

        TableMetadataManager.update_uuid(table_local_metadata_path, new_local_uuid)

        if not is_table_engine:
            logging.info(
                "Table {}.{} has engine={}. Don't need move in local store.",
                database,
                table,
                engine,
            )
            return

        try:
            TableMetadataManager.move_table_store(old_table_uuid, new_local_uuid)
        except Exception:
            logging.error(
                "Failed move_table_local_store. old uuid={}, new_local_uuid={}. Need restore uuid in metadata for table={}.",
                old_table_uuid,
                new_local_uuid,
                f"{database}.{table}",
            )
            raise

        logging.info(
            "Local table store {}.{} was moved from {} to {}",
            database,
            table,
            old_table_uuid,
            new_local_uuid,
        )


def read_local_table_metadata(ctx: "Context", table_local_metadata_path: str) -> str:
    """Read local table metadata file content."""
    if match_ch_version(ctx, "25.1"):
        table_local_metadata_path = f"{CLICKHOUSE_PATH}/{table_local_metadata_path}"

    with open(table_local_metadata_path, "r", encoding="utf-8") as f:
        return f.read()
