import re
import uuid
from enum import Enum

UUID_PATTERN = "UUID"
ENGINE_PATTERN = "ENGINE"


class MergeTreeFamilyEngines(Enum):
    MERGE_TREE = "MergeTree"
    REPLACING_MERGE_TREE = "ReplacingMergeTree"
    SUMMING_MERGE_TREE = "SummingMergeTree"
    AGGREGATING_MERGE_TREE = "AggregatingMergeTree"
    COLLAPSING_MERGE_TREE = "CollapsingMergeTree"
    VERSIONED_MERGE_TREE = "VersionedCollapsingMergeTree"
    REPLICATED_MERGE_TREE = "ReplicatedMergeTree"

    @staticmethod
    def from_str(engine_str: str) -> "MergeTreeFamilyEngines":
        for engine in MergeTreeFamilyEngines:
            if engine.value == engine_str:
                return engine
        raise RuntimeError(
            f"Doesn't support removing detached table with engine {engine_str}"
        )


class TableMetadataParser:
    @property
    def table_uuid(self):
        return self._table_uuid

    @property
    def table_engine(self):
        return self._table_engine

    def __init__(self, table_metadata_path: str):
        assert table_metadata_path.endswith(".sql")

        self._table_uuid = None
        self._table_engine = None

        with open(table_metadata_path, "r", encoding="utf-8") as metadata_file:
            for line in metadata_file:
                if line.startswith("ATTACH TABLE") and UUID_PATTERN in line:
                    assert self._table_uuid is None
                    self._table_uuid = TableMetadataParser._parse_uuid(line)
                    continue
                if line.startswith("ENGINE ="):
                    assert self._table_engine is None
                    self._table_engine = TableMetadataParser._parse_engine(line)

                    if (
                        self.table_engine
                        == MergeTreeFamilyEngines.REPLICATED_MERGE_TREE
                    ):
                        # todo parse logic for zk in future.
                        pass

                    break

        if self._table_uuid is None:
            raise RuntimeError(f"Empty UUID from metadata: '{table_metadata_path}'")

        if self._table_engine is None:
            raise RuntimeError(
                f"Empty table engine from metadata: '{table_metadata_path}'"
            )

    @staticmethod
    def _is_valid_uuid(uuid_str: str) -> bool:
        try:
            val = uuid.UUID(uuid_str)
        except ValueError:
            return False
        return str(val) == uuid_str

    @staticmethod
    def _parse_uuid(line: str) -> str:
        uuid_pattern = re.compile(r"UUID\s+'([a-f0-9-]+)'", re.IGNORECASE)
        match = uuid_pattern.search(line)

        if not match:
            raise RuntimeError("Failed parse UUID from metadata.")

        result = match.group(1)
        if not TableMetadataParser._is_valid_uuid(result):
            raise RuntimeError("Failed parse UUID from metadata.")
        return result

    @staticmethod
    def _parse_engine(line: str) -> MergeTreeFamilyEngines:
        pattern = re.compile(r"ENGINE = (\w+)")

        match = pattern.search(line)
        if not match:
            raise RuntimeError(f"Failed parse {ENGINE_PATTERN} from metadata.")

        return MergeTreeFamilyEngines.from_str(match.group(1))
