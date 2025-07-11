import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

from ch_tools.chadmin.cli import metadata
from ch_tools.chadmin.internal.clickhouse_disks import CLICKHOUSE_METADATA_PATH

PATTERN_UUID_FROM_METADATA = r"(ATTACH\s+\w+\s+(?:\w+\s+)*UUID\s+')([^']+)('.*)"


class DatabaseEngine(Enum):
    ATOMIC = "Atomic"
    REPLICATED = "Replicated"

    def is_replicated(self) -> bool:
        return self == DatabaseEngine.REPLICATED

    @classmethod
    def from_str(cls, engine_str: str) -> "DatabaseEngine":
        engine = engine_str.strip().lower()
        for supported_engine in cls:
            if supported_engine.value.lower() == engine:
                return supported_engine
        raise ValueError(f"Unknown DatabaseEngine: {engine_str}")

    def __str__(self) -> str:
        return self.value


@dataclass
class DatabaseMetadata:
    database_name: str
    database_uuid: str
    database_engine: DatabaseEngine
    replica_path: Optional[str] = None
    shard: Optional[str] = None
    replica_name: Optional[str] = None

    def set_engine_from(self, db_metadata: "DatabaseMetadata") -> None:
        self.database_engine = db_metadata.database_engine
        self.replica_path = db_metadata.replica_path
        self.shard = db_metadata.shard
        self.replica_name = db_metadata.replica_name

    def update_metadata_file(self):
        file_path = db_metadata_path(self.database_name)

        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        if self.database_engine == DatabaseEngine.REPLICATED:
            engine_line = f"ENGINE = Replicated('{self.replica_path}', '{self.shard}', '{ self.replica_name}')"
        else:
            engine_line = "ENGINE = Atomic"

        lines[1] = engine_line

        with open(file_path, "w", encoding="utf-8") as f:
            f.writelines(lines)

    def set_replicated(self) -> None:
        self.database_engine = DatabaseEngine.REPLICATED
        self.replica_path = f"/clickhouse/{self.database_name}"
        self.shard = "{shard}"
        self.replica_name = "{replica}"

        self.update_metadata_file()

    def set_atomic(self) -> None:
        self.database_engine = DatabaseEngine.ATOMIC
        self.replica_path = None
        self.shard = None
        self.replica_name = None

        self.update_metadata_file()


def db_metadata_path(database_name: str) -> str:
    return CLICKHOUSE_METADATA_PATH + f"/{database_name}.sql"


def parse_database_from_metadata(database_name: str) -> DatabaseMetadata:
    database_metadata_path = db_metadata_path(database_name)

    assert database_metadata_path.endswith(".sql")
    database_uuid = None
    database_engine = None
    replica_path = None
    shard = None
    replica_name = None

    with open(database_metadata_path, "r", encoding="utf-8") as metadata_file:
        for line in metadata_file:
            if line.startswith("ATTACH DATABASE") and metadata.UUID_TOKEN in line:
                assert database_uuid is None
                database_uuid = metadata.parse_uuid(line)
            if line.startswith("ENGINE ="):
                assert database_engine is None
                database_engine = _parse_engine(line)
                if database_engine.is_replicated():
                    replica_path, shard, replica_name = _parse_database_replica_params(
                        line
                    )

    if database_uuid is None:
        raise RuntimeError(f"Empty UUID from metadata: '{database_metadata_path}'")

    if database_engine is None:
        raise RuntimeError(
            f"Empty database engine from metadata: '{database_metadata_path}'"
        )

    return DatabaseMetadata(
        database_name,
        database_uuid,
        database_engine,
        replica_path=replica_path,
        shard=shard,
        replica_name=replica_name,
    )


def _parse_engine(line: str) -> DatabaseEngine:
    pattern = re.compile(r"ENGINE = (\w+)")

    match = pattern.search(line)
    if not match:
        raise RuntimeError(f"Failed parse {metadata.ENGINE_TOKEN} from metadata.")

    return DatabaseEngine(match.group(1))


def _parse_database_replica_params(line: str) -> Tuple[str, str, str]:
    pattern = r"'([^']*)'"
    matches = re.findall(pattern, line)

    if len(matches) != 3:
        raise ValueError(f"Failed parse metadata for replicated engine: {len(matches)}")
    return matches[0], matches[1], matches[2]


def remove_uuid_from_metadata(text_metadata: str) -> str:
    result = re.sub(PATTERN_UUID_FROM_METADATA, r"\1\3", text_metadata)
    return result
