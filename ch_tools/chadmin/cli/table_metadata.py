import re
import uuid

UUID_PATTERN = "UUID"
OFFSET_UUID_PATTERN = 1
ENGINE_PATTERN = "ENGINE"

PARSER_SUPPORT_TABLE_ENGINE = [
    "MergeTree",
    "ReplacingMergeTree",
    "SummingMergeTree",
    "AggregatingMergeTree",
    "CollapsingMergeTree",
    "VersionedCollapsingMergeTree",
    "ReplicatedMergeTree",
]


class TableMetadata:
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
                if UUID_PATTERN in line:
                    self._table_uuid = TableMetadata._parse_uuid(line)
                    continue
                if ENGINE_PATTERN in line:
                    result = TableMetadata._parse_engine(line)
                    self._table_engine = result
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
    def _is_valid_engine(engine: str) -> bool:
        return engine in PARSER_SUPPORT_TABLE_ENGINE

    @staticmethod
    def _parse_uuid(line: str) -> str:
        result = ""
        parts = line.split()
        try:
            index = parts.index(UUID_PATTERN)
            if len(parts) < index + OFFSET_UUID_PATTERN + 1:
                raise RuntimeError("Failed parse UUID from metadata.")

            result = parts[index + OFFSET_UUID_PATTERN].strip("'")
        except ValueError:
            raise RuntimeError(f"Failed parse {UUID_PATTERN} from metadata.")

        if not TableMetadata._is_valid_uuid(result):
            raise RuntimeError("Failed parse UUID from metadata.")

        return result

    @staticmethod
    def _parse_engine(line: str) -> str:
        pattern = re.compile(r"ENGINE = (\w+)")

        match = pattern.search(line)
        if not match:
            raise RuntimeError(f"Failed parse {ENGINE_PATTERN} from metadata.")

        engine = match.group(1)

        assert TableMetadata._is_valid_engine(engine)
        return engine
