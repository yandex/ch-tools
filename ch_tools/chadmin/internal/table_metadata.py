import grp
import os
import pwd
import re
from enum import Enum
from typing import Tuple

from click import Context

from ch_tools.chadmin.cli import metadata
from ch_tools.chadmin.internal.clickhouse_disks import CLICKHOUSE_PATH
from ch_tools.chadmin.internal.zookeeper import get_zk_node
from ch_tools.common import logging

REPLICATED_MERGE_TREE_PATTERN = r"ReplicatedMergeTree\([^\)]*\)"


class MergeTreeFamilyEngines(Enum):
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


def _parse_replica_params(line: str) -> Tuple[str, str]:
    pattern = r"ENGINE = Replicated\w*MergeTree\('([^']*)', '([^']*)'(?:, [^)]*)?\)"
    match = re.match(pattern, line)

    if not match:
        raise RuntimeError("Failed parse replicated params from metadata.")

    path = match.group(1)
    name = match.group(2)
    return path, name


def check_replica_path_contains_macros(path: str, macros: str) -> bool:
    return f"{{{macros}}}" in path


def update_uuid_table_metadata_file(
    table_local_metadata_path: str, new_uuid: str
) -> None:
    with open(table_local_metadata_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    assert len(lines[0]) != 0

    lines[0] = re.sub(metadata.UUID_PATTERN, f"UUID '{new_uuid}'", lines[0])

    with open(table_local_metadata_path, "w", encoding="utf-8") as f:
        f.writelines(lines)


def get_table_store_path(table_uuid: str) -> str:
    return f"{CLICKHOUSE_PATH}/store/{table_uuid[:3]}/{table_uuid}"


def move_table_local_store(old_table_uuid: str, new_uuid: str) -> None:
    old_table_store_path = get_table_store_path(old_table_uuid)
    logging.info("old_table_store_path={}", old_table_store_path)

    assert os.path.exists(old_table_store_path)

    target = f"{CLICKHOUSE_PATH}/store/{new_uuid[:3]}"

    if not os.path.exists(target):
        logging.info("need create path={}", target)
        os.mkdir(target)
        os.chmod(target, 0o750)
        uid = pwd.getpwnam("clickhouse").pw_uid
        gid = grp.getgrnam("clickhouse").gr_gid

        os.chown(target, uid, gid)
    else:
        logging.info("path exists: {}", target)

    new_table_store_path = get_table_store_path(new_uuid)
    logging.info("new_table_store_path={}", new_table_store_path)

    os.rename(old_table_store_path, new_table_store_path)

    uid = pwd.getpwnam("clickhouse").pw_uid
    gid = grp.getgrnam("clickhouse").gr_gid
    os.chown(new_table_store_path, uid, gid)


def get_table_shared_id(ctx: Context, zookeeper_path: str) -> str:
    table_shared_id_node_path = zookeeper_path + "/table_shared_id"

    table_uuid = get_zk_node(ctx, table_shared_id_node_path)
    logging.info("zk_path={} contains table_shared_id={}", zookeeper_path, table_uuid)

    return table_uuid


def remove_replicated_params(create_table_query: str) -> str:
    return re.sub(
        REPLICATED_MERGE_TREE_PATTERN, "ReplicatedMergeTree", create_table_query
    )
