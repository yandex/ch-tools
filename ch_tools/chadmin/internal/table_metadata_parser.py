"""Table metadata parser utilities."""

import os
import re
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

# Constants for UUID parsing
UUID_TOKEN = "UUID"
ENGINE_TOKEN = "ENGINE"
UUID_PATTERN = re.compile(r"UUID\s+'([a-f0-9-]+)'", re.IGNORECASE)


def _is_valid_uuid(uuid_str: str) -> bool:
    """Check if string is a valid UUID."""
    try:
        val = uuid.UUID(uuid_str)
    except ValueError:
        return False
    return str(val) == uuid_str


def parse_uuid(line: str) -> str:
    """Parse UUID from metadata line."""
    match = UUID_PATTERN.search(line)

    if not match:
        raise RuntimeError("Failed parse UUID from metadata.")

    result = match.group(1)
    if not _is_valid_uuid(result):
        raise RuntimeError("Failed parse UUID from metadata.")
    return result


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
                    if line.startswith("ATTACH TABLE") and UUID_TOKEN in line:
                        if table_uuid is not None:
                            raise MetadataParseError(
                                f"Duplicate UUID found in metadata: '{table_metadata_path}'"
                            )
                        table_uuid = parse_uuid(line)
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


# Backward compatibility function
def parse_table_metadata(table_metadata_path: str) -> TableMetadata:
    """Parse table metadata (backward compatibility)."""
    return TableMetadataParser.parse(table_metadata_path)
