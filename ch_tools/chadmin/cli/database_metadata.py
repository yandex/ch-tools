import re
from enum import Enum
from typing import Tuple

from ch_tools.chadmin.cli import metadata


class DatabaseEngine(Enum):
    ATOMIC = "Atomic"
    REPLICATED = "Replicated"

    @staticmethod
    def from_str(engine_str: str) -> "DatabaseEngine":
        for engine in DatabaseEngine:
            if engine.value == engine_str:
                return engine
        raise RuntimeError(f"Engine {engine_str} doesn't exist in DatabaseEngine.")

    def is_replicated(self) -> bool:
        return self == DatabaseEngine.REPLICATED


class DatabaseMetadata:
    def __init__(
        self,
        database_uuid,
        database_engine,
        replica_path=None,
        shard=None,
        replica_name=None,
    ):
        self.database_uuid = database_uuid
        self.database_engine = database_engine
        self.replica_path = replica_path
        self.shard = shard
        self.replica_name = replica_name

    def set_engine_from(self, db_metadata):
        self.database_engine = db_metadata.database_engine
        self.replica_path = db_metadata.replica_path
        self.shard = db_metadata.shard
        self.replica_name = db_metadata.replica_name

    def update_metadata_file(self, file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        engine_line = f"ENGINE = Replicated('{self.replica_path}', '{self.shard}', '{ self.replica_name}')"
        lines[1] = engine_line

        with open(file_path, "w", encoding="utf-8") as f:
            f.writelines(lines)


def parse_database_metadata(database_metadata_path: str) -> DatabaseMetadata:
    assert database_metadata_path.endswith(".sql")
    database_uuid = None
    database_engine = None
    replica_path = None
    shard = None
    replica_name = None

    with open(database_metadata_path, "r", encoding="utf-8") as metadata_file:
        for line in metadata_file:
            if line.startswith("ATTACH DATABASE") and metadata.UUID_PATTERN in line:
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
        raise RuntimeError(f"Failed parse {metadata.ENGINE_PATTERN} from metadata.")

    return DatabaseEngine.from_str(match.group(1))


def _parse_database_replica_params(line: str) -> Tuple[str, str, str]:
    pattern = r"'([^']*)'"
    matches = re.findall(pattern, line)

    if len(matches) != 3:
        raise ValueError(
            "Failed parse metadata for replicated engine: {}".format(len(matches))
        )
    return matches[0], matches[1], matches[2]
