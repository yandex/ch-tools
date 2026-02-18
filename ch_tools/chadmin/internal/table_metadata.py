"""
Table metadata parsing and management utilities.

This module provides classes and functions for working with ClickHouse table metadata files,
including parsing metadata, managing UUIDs, and handling table storage operations.
"""

import grp
import os
import pwd
import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

from click import Context

from ch_tools.chadmin.cli import table_metadata_parser
from ch_tools.chadmin.internal.clickhouse_disks import CLICKHOUSE_PATH
from ch_tools.chadmin.internal.zookeeper import get_zk_node
from ch_tools.common import logging

REPLICATED_MERGE_TREE_PATTERN = r"ReplicatedMergeTree\([^\)]*\)"


class TableMetadataError(Exception):
    """Base exception for table metadata operations."""

    pass


class MetadataParseError(TableMetadataError):
    """Exception raised when metadata parsing fails."""

    pass


class MetadataFileError(TableMetadataError):
    """Exception raised when metadata file operations fail."""

    pass


class MergeTreeFamilyEngines(Enum):
    """Enumeration of MergeTree family table engines."""

    MERGE_TREE = "MergeTree"
    REPLACING_MERGE_TREE = "ReplacingMergeTree"
    SUMMING_MERGE_TREE = "SummingMergeTree"
    AGGREGATING_MERGE_TREE = "AggregatingMergeTree"
    COLLAPSING_MERGE_TREE = "CollapsingMergeTree"
    VERSIONED_MERGE_TREE = "VersionedCollapsingMergeTree"
    GRAPHITE_MERGE_TREE = "GraphiteMergeTree"
    DISTRIBUTED = "Distributed"
    REPLICATED_MERGE_TREE = "ReplicatedMergeTree"
    REPLICATED_SUMMING_MERGE_TREE = "ReplicatedSummingMergeTree"
    REPLICATED_REPLACING_MERGE_TREE = "ReplicatedReplacingMergeTree"
    REPLICATED_AGGREGATING_MERGE_TREE = "ReplicatedAggregatingMergeTree"
    REPLICATED_COLLAPSING_MERGE_TREE = "ReplicatedCollapsingMergeTree"
    REPLICATED_VERSIONED_MERGE_TREE = "ReplicatedVersionedCollapsingMergeTree"
    REPLICATED_GRAPHITE_MERGE_TREE = "ReplicatedGraphiteMergeTree"

    @staticmethod
    def from_str(engine_str: str) -> "MergeTreeFamilyEngines":
        """
        Convert string representation to MergeTreeFamilyEngines enum.

        Args:
            engine_str: String representation of the engine

        Returns:
            MergeTreeFamilyEngines enum value

        Raises:
            MetadataParseError: If engine string is not recognized
        """
        for engine in MergeTreeFamilyEngines:
            if engine.value == engine_str:
                return engine
        raise MetadataParseError(
            f"Engine '{engine_str}' is not a valid MergeTreeFamilyEngines value"
        )

    def is_table_engine_replicated(self) -> bool:
        """
        Check if the engine is a replicated variant.

        Returns:
            True if engine is replicated, False otherwise
        """
        engines_list = list(MergeTreeFamilyEngines)
        replicated_start_idx = engines_list.index(
            MergeTreeFamilyEngines.REPLICATED_MERGE_TREE
        )
        return self.value in [
            engine.value for engine in engines_list[replicated_start_idx:]
        ]


@dataclass
class TableMetadata:
    """
    Dataclass representing parsed table metadata.

    Attributes:
        table_uuid: UUID of the table
        table_engine: Table engine type
        replica_path: ZooKeeper path for replicated tables (optional)
        replica_name: Replica name for replicated tables (optional)
    """

    table_uuid: str
    table_engine: MergeTreeFamilyEngines
    replica_path: Optional[str] = None
    replica_name: Optional[str] = None

    def is_replicated(self) -> bool:
        """Check if table uses replicated engine."""
        return self.table_engine.is_table_engine_replicated()


class TableMetadataParser:
    """Parser for ClickHouse table metadata files."""

    @staticmethod
    def parse(table_metadata_path: str) -> TableMetadata:
        """
        Parse table metadata from a .sql file.

        Args:
            table_metadata_path: Path to the metadata file

        Returns:
            TableMetadata object with parsed information

        Raises:
            MetadataParseError: If parsing fails or required fields are missing
            MetadataFileError: If file cannot be read
        """
        if not table_metadata_path.endswith(".sql"):
            raise MetadataParseError(
                f"Metadata file must have .sql extension: '{table_metadata_path}'"
            )

        if not os.path.exists(table_metadata_path):
            raise MetadataFileError(f"Metadata file not found: '{table_metadata_path}'")

        table_uuid = None
        table_engine = None
        replica_path = None
        replica_name = None

        try:
            with open(table_metadata_path, "r", encoding="utf-8") as metadata_file:
                for line in metadata_file:
                    if (
                        line.startswith("ATTACH TABLE")
                        and table_metadata_parser.UUID_TOKEN in line
                    ):
                        if table_uuid is not None:
                            raise MetadataParseError(
                                f"Duplicate UUID found in metadata: '{table_metadata_path}'"
                            )
                        table_uuid = table_metadata_parser.parse_uuid(line)
                    if line.startswith("ENGINE ="):
                        if table_engine is not None:
                            raise MetadataParseError(
                                f"Duplicate ENGINE found in metadata: '{table_metadata_path}'"
                            )
                        table_engine = TableMetadataParser._parse_engine(line)
                        if table_engine.is_table_engine_replicated():
                            replica_path, replica_name = (
                                TableMetadataParser._parse_replica_params(line)
                            )
        except OSError as e:
            raise MetadataFileError(
                f"Failed to read metadata file '{table_metadata_path}': {e}"
            ) from e

        if table_uuid is None:
            raise RuntimeError(
                f"Empty UUID from table metadata: '{table_metadata_path}'"
            )

        if table_engine is None:
            raise RuntimeError(
                f"Empty table engine from table metadata: '{table_metadata_path}'"
            )

        return TableMetadata(table_uuid, table_engine, replica_path, replica_name)

    @staticmethod
    def _parse_engine(line: str) -> MergeTreeFamilyEngines:
        """
        Parse engine type from ENGINE line.

        Args:
            line: Line containing ENGINE declaration

        Returns:
            MergeTreeFamilyEngines enum value

        Raises:
            MetadataParseError: If engine cannot be parsed
        """
        pattern = re.compile(r"ENGINE = (\w+)")
        match = pattern.search(line)
        if not match:
            raise MetadataParseError(
                f"Failed to parse ENGINE from line: '{line.strip()}'"
            )

        return MergeTreeFamilyEngines.from_str(match.group(1))

    @staticmethod
    def _parse_replica_params(line: str) -> Tuple[str, str]:
        """
        Parse replica path and name from ReplicatedMergeTree ENGINE line.

        Args:
            line: Line containing ReplicatedMergeTree ENGINE declaration

        Returns:
            Tuple of (replica_path, replica_name)

        Raises:
            MetadataParseError: If replica parameters cannot be parsed
        """
        pattern = r"ENGINE = Replicated\w*MergeTree\('([^']*)', '([^']*)'(?:, [^)]*)?\)"
        match = re.match(pattern, line)

        if not match:
            raise MetadataParseError(
                f"Failed to parse replicated parameters from line: '{line.strip()}'"
            )

        path = match.group(1)
        name = match.group(2)
        return path, name


class TableMetadataManager:
    """Manager for table metadata file operations."""

    @staticmethod
    def update_uuid(table_local_metadata_path: str, new_uuid: str) -> None:
        """
        Update UUID in table metadata file.

        Args:
            table_local_metadata_path: Path to the metadata file
            new_uuid: New UUID to set

        Raises:
            MetadataFileError: If file operations fail
        """
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

            lines[0] = re.sub(
                table_metadata_parser.UUID_PATTERN, f"UUID '{new_uuid}'", lines[0]
            )

            with open(table_local_metadata_path, "w", encoding="utf-8") as f:
                f.writelines(lines)

            logging.debug("UUID successfully updated in metadata file")
        except OSError as e:
            raise MetadataFileError(
                f"Failed to update UUID in metadata file '{table_local_metadata_path}': {e}"
            ) from e

    @staticmethod
    def get_table_store_path(table_uuid: str) -> str:
        """
        Get the storage path for a table based on its UUID.

        Args:
            table_uuid: Table UUID

        Returns:
            Full path to table storage directory
        """
        return f"{CLICKHOUSE_PATH}/store/{table_uuid[:3]}/{table_uuid}"

    @staticmethod
    def move_table_store(old_table_uuid: str, new_uuid: str) -> None:
        """
        Move table storage directory to new UUID location.

        Args:
            old_table_uuid: Current table UUID
            new_uuid: New table UUID

        Raises:
            MetadataFileError: If move operation fails
        """
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
    def get_table_shared_id(ctx: Context, zookeeper_path: str) -> str:
        """
        Get table shared ID from ZooKeeper.

        Args:
            ctx: Click context
            zookeeper_path: ZooKeeper path to table

        Returns:
            Table shared ID (UUID)

        Raises:
            MetadataError: If shared ID cannot be retrieved
        """
        table_shared_id_node_path = zookeeper_path + "/table_shared_id"

        try:
            table_uuid = get_zk_node(ctx, table_shared_id_node_path)
            logging.debug(
                "Retrieved table_shared_id={} from zk_path={}",
                table_uuid,
                zookeeper_path,
            )
            return table_uuid
        except Exception as e:
            raise TableMetadataError(
                f"Failed to get table_shared_id from ZooKeeper path '{table_shared_id_node_path}': {e}"
            ) from e


# Backward compatibility functions
def parse_table_metadata(table_metadata_path: str) -> TableMetadata:
    """
    Parse table metadata from a .sql file (backward compatibility wrapper).

    Args:
        table_metadata_path: Path to the metadata file

    Returns:
        TableMetadata object with parsed information
    """
    return TableMetadataParser.parse(table_metadata_path)


def update_uuid_table_metadata_file(
    table_local_metadata_path: str, new_uuid: str
) -> None:
    """
    Update UUID in table metadata file (backward compatibility wrapper).

    Args:
        table_local_metadata_path: Path to the metadata file
        new_uuid: New UUID to set
    """
    TableMetadataManager.update_uuid(table_local_metadata_path, new_uuid)


def get_table_store_path(table_uuid: str) -> str:
    """
    Get the storage path for a table (backward compatibility wrapper).

    Args:
        table_uuid: Table UUID

    Returns:
        Full path to table storage directory
    """
    return TableMetadataManager.get_table_store_path(table_uuid)


def move_table_local_store(old_table_uuid: str, new_uuid: str) -> None:
    """
    Move table storage directory (backward compatibility wrapper).

    Args:
        old_table_uuid: Current table UUID
        new_uuid: New table UUID
    """
    TableMetadataManager.move_table_store(old_table_uuid, new_uuid)


def get_table_shared_id(ctx: Context, zookeeper_path: str) -> str:
    """
    Get table shared ID from ZooKeeper (backward compatibility wrapper).

    Args:
        ctx: Click context
        zookeeper_path: ZooKeeper path to table

    Returns:
        Table shared ID (UUID)
    """
    return TableMetadataManager.get_table_shared_id(ctx, zookeeper_path)


def check_replica_path_contains_macros(path: str, macros: str) -> bool:
    """
    Check if replica path contains specified macros.

    Args:
        path: Replica path to check
        macros: Macros name to look for

    Returns:
        True if macros is present in path, False otherwise
    """
    return f"{{{macros}}}" in path


def remove_replicated_params(create_table_query: str) -> str:
    """
    Remove replicated parameters from CREATE TABLE query.

    Args:
        create_table_query: CREATE TABLE query string

    Returns:
        Query with replicated parameters removed
    """
    return re.sub(
        REPLICATED_MERGE_TREE_PATTERN, "ReplicatedMergeTree", create_table_query
    )


def is_table(engine: str) -> bool:
    """
    Check if engine represents a table (not a view).

    Args:
        engine: Engine name

    Returns:
        True if engine is a table, False otherwise
    """
    logging.debug("Checking if engine '{}' is a table", engine)
    return engine not in ["View", "MaterializedView"]


def is_view(engine: str) -> bool:
    """
    Check if engine represents a view.

    Args:
        engine: Engine name

    Returns:
        True if engine is a view, False otherwise
    """
    logging.debug("Checking if engine '{}' is a view", engine)
    return engine == "View"
