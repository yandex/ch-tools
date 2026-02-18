"""Table metadata utilities."""

import grp
import os
import pwd
import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

from ch_tools.chadmin.cli import table_metadata_parser
from ch_tools.chadmin.internal.clickhouse_disks import CLICKHOUSE_PATH
from ch_tools.common import logging


class TableMetadataError(Exception):
    """Base exception for metadata operations."""

    pass


class MetadataParseError(TableMetadataError):
    """Metadata parsing error."""

    pass


class MetadataFileError(TableMetadataError):
    """Metadata file operation error."""

    pass


class MergeTreeFamilyEngines(Enum):
    """MergeTree family engines."""

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
        """Convert string to engine enum."""
        for engine in MergeTreeFamilyEngines:
            if engine.value == engine_str:
                return engine
        raise MetadataParseError(
            f"Engine '{engine_str}' is not a valid MergeTreeFamilyEngines value"
        )

    def is_table_engine_replicated(self) -> bool:
        """Check if engine is replicated."""
        engines_list = list(MergeTreeFamilyEngines)
        replicated_start_idx = engines_list.index(
            MergeTreeFamilyEngines.REPLICATED_MERGE_TREE
        )
        return self.value in [
            engine.value for engine in engines_list[replicated_start_idx:]
        ]


@dataclass
class TableMetadata:
    """Table metadata."""

    table_uuid: str
    table_engine: MergeTreeFamilyEngines
    replica_path: Optional[str] = None
    replica_name: Optional[str] = None

    def is_replicated(self) -> bool:
        """Check if table is replicated."""
        return self.table_engine.is_table_engine_replicated()


class TableMetadataParser:
    """Table metadata file parser."""

    @staticmethod
    def parse(table_metadata_path: str) -> TableMetadata:
        """Parse metadata from .sql file."""
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
        """Parse engine type from ENGINE line."""
        pattern = re.compile(r"ENGINE = (\w+)")
        match = pattern.search(line)
        if not match:
            raise MetadataParseError(
                f"Failed to parse ENGINE from line: '{line.strip()}'"
            )

        return MergeTreeFamilyEngines.from_str(match.group(1))

    @staticmethod
    def _parse_replica_params(line: str) -> Tuple[str, str]:
        """Parse replica path and name from ENGINE line."""
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
    """Metadata file operations manager."""

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


# Backward compatibility functions
def parse_table_metadata(table_metadata_path: str) -> TableMetadata:
    """Parse table metadata (backward compatibility)."""
    return TableMetadataParser.parse(table_metadata_path)
