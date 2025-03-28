import re
from enum import Enum
from typing import Tuple

from ch_tools.chadmin.cli import metadata

UUID_PATTERN = re.compile(
    r"UUID '[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}'"
)


class MergeTreeFamilyEngines(Enum):
    MERGE_TREE = "MergeTree"
    REPLACING_MERGE_TREE = "ReplacingMergeTree"
    SUMMING_MERGE_TREE = "SummingMergeTree"
    AGGREGATING_MERGE_TREE = "AggregatingMergeTree"
    COLLAPSING_MERGE_TREE = "CollapsingMergeTree"
    VERSIONED_MERGE_TREE = "VersionedCollapsingMergeTree"
    GRAPHITE_MERGE_TREE = "GraphiteMergeTree"
    REPLICATED_MERGE_TREE = "ReplicatedMergeTree"
    REPLICATED_SUMMING_MERGE_TREE = "ReplicatedSummingMergeTree"
    REPLICATED_REPLACING_MERGE_TREE = "ReplicatedReplacingMergeTree"
    REPLICATED_AGGREGATING_MERGE_TREE = "ReplicatedAggregatingMergeTree"
    REPLICATED_COLLAPSING_MERGE_TREE = "ReplicatedCollapsingMergeTree"
    REPLICATED_VERSIONED_MERGE_TREE = "ReplicatedVersionedCollapsingMergeTree"
    REPLICATED_GRAPHITE_MERGE_TREE = "ReplicatedGraphiteMergeTree"

    @staticmethod
    def from_str(engine_str: str) -> "MergeTreeFamilyEngines":
        for engine in MergeTreeFamilyEngines:
            if engine.value == engine_str:
                return engine
        raise RuntimeError(
            f"Engine {engine_str} doesn't exist in MergeTreeFamilyEngines."
        )

    def is_table_engine_replicated(self) -> bool:
        engines_list = list(MergeTreeFamilyEngines)
        replicated_start_idx = engines_list.index(
            MergeTreeFamilyEngines.REPLICATED_MERGE_TREE
        )
        return self.value in [
            engine.value for engine in engines_list[replicated_start_idx:]
        ]


class TableMetadata:
    def __init__(self, table_uuid, table_engine, replica_path=None, replica_name=None):
        self.table_uuid = table_uuid
        self.table_engine = table_engine
        self.replica_path = replica_path
        self.replica_name = replica_name


def parse_table_metadata(table_metadata_path: str) -> TableMetadata:
    assert table_metadata_path.endswith(".sql")
    table_uuid = None
    table_engine = None
    replica_path = None
    replica_name = None

    with open(table_metadata_path, "r", encoding="utf-8") as metadata_file:
        for line in metadata_file:
            if line.startswith("ATTACH TABLE") and metadata.UUID_TOKEN in line:
                assert table_uuid is None
                table_uuid = metadata.parse_uuid(line)
            if line.startswith("ENGINE ="):
                assert table_engine is None
                table_engine = _parse_engine(line)
                if table_engine.is_table_engine_replicated():
                    replica_path, replica_name = _parse_replica_params(line)

    if table_uuid is None:
        raise RuntimeError(f"Empty UUID from metadata: '{table_metadata_path}'")

    if table_engine is None:
        raise RuntimeError(f"Empty table engine from metadata: '{table_metadata_path}'")

    return TableMetadata(table_uuid, table_engine, replica_path, replica_name)


def _parse_engine(line: str) -> MergeTreeFamilyEngines:
    pattern = re.compile(r"ENGINE = (\w+)")

    match = pattern.search(line)
    if not match:
        raise RuntimeError(f"Failed parse {metadata.ENGINE_TOKEN} from metadata.")

    return MergeTreeFamilyEngines.from_str(match.group(1))


# table_replca?
def _parse_replica_params(line: str) -> Tuple[str, str]:
    pattern = r"ENGINE = Replicated\w*MergeTree\('([^']*)', '([^']*)'(?:, [^)]*)?\)"
    match = re.match(pattern, line)

    if not match:
        raise RuntimeError("Failed parse replicated params from metadata.")

    path = match.group(1)
    name = match.group(2)
    return path, name


# @todo move to table/replica?
def check_replica_path_contains_macros(path: str, macros: str) -> bool:
    return f"{{{macros}}}" in path


def update_uuid_table_metadata_file(
    table_local_metadata_path: str, new_uuid: str
) -> None:
    with open(table_local_metadata_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    assert len(lines[0]) != 0

    lines[0] = re.sub(UUID_PATTERN, f"UUID '{new_uuid}'", lines[0])

    with open(table_local_metadata_path, "w", encoding="utf-8") as f:
        f.writelines(lines)
