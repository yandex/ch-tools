import pytest

from ch_tools.chadmin.internal.table_metadata import (
    MergeTreeFamilyEngines,
    check_replica_path_contains_macros,
    parse_table_metadata,
)

PATH_TO_TESTS = "tests/unit/common/clickhouse/"


@pytest.mark.parametrize(
    "file,expected",
    [
        pytest.param(
            "metadata/table_merge_tree.sql",
            {
                "table_uuid": "9612256b-b461-4df5-8015-72f9727d1f95",
                "table_engine": MergeTreeFamilyEngines.MERGE_TREE,
            },
            id="MergeTree",
        ),
        pytest.param(
            "metadata/table_merge_tree_field_uuid.sql",
            {
                "table_uuid": "9612256b-b461-4df5-8015-72f9727d1f95",
                "table_engine": MergeTreeFamilyEngines.MERGE_TREE,
            },
            id="MergeTree with field uuid",
        ),
        pytest.param(
            "metadata/table_merge_tree_field_engine.sql",
            {
                "table_uuid": "9612256b-b461-4df5-8015-72f9727d1f95",
                "table_engine": MergeTreeFamilyEngines.MERGE_TREE,
            },
            id="MergeTree with field engine",
        ),
        pytest.param(
            "metadata/table_replacing_merge_tree.sql",
            {
                "table_uuid": "c322e832-2628-45f9-b2f5-fd659078c5c2",
                "table_engine": MergeTreeFamilyEngines.REPLACING_MERGE_TREE,
            },
            id="ReplacingMergeTree",
        ),
        pytest.param(
            "metadata/table_summing_merge_tree.sql",
            {
                "table_uuid": "5f55555d-c9f7-47ac-8d87-b3ac8a889161",
                "table_engine": MergeTreeFamilyEngines.SUMMING_MERGE_TREE,
            },
            id="SummingMergeTree",
        ),
        pytest.param(
            "metadata/table_aggregating_merge_tree.sql",
            {
                "table_uuid": "40c7c9a8-7451-4a10-8b43-443436f33413",
                "table_engine": MergeTreeFamilyEngines.AGGREGATING_MERGE_TREE,
            },
            id="AggregatingMergeTree",
        ),
        pytest.param(
            "metadata/table_collapsing_merge_tree.sql",
            {
                "table_uuid": "122b369e-3866-4c2b-8ca1-9e07c75ecee0",
                "table_engine": MergeTreeFamilyEngines.COLLAPSING_MERGE_TREE,
            },
            id="CollapsingMergeTree",
        ),
        pytest.param(
            "metadata/table_versioned_collapsing_merge_tree.sql",
            {
                "table_uuid": "42089e02-c13c-4b52-a1bf-4f9aa3e84e56",
                "table_engine": MergeTreeFamilyEngines.VERSIONED_MERGE_TREE,
            },
            id="VersionedCollapsingMergeTree",
        ),
    ],
)
def test_parse_table_metadata_non_repl(file, expected):
    metadata = parse_table_metadata(table_metadata_path=PATH_TO_TESTS + file)
    assert metadata.table_uuid == expected["table_uuid"]
    assert metadata.table_engine == expected["table_engine"]
    assert metadata.replica_name is None
    assert metadata.replica_path is None


@pytest.mark.parametrize(
    "file,expected",
    [
        pytest.param(
            "metadata/table_replicated_merge_tree.sql",
            {
                "table_uuid": "f438d816-605d-4fe0-a9cb-4edba3ce72dd",
                "table_engine": MergeTreeFamilyEngines.REPLICATED_MERGE_TREE,
                "replica_path": "/clickhouse/tables/{shard}/test_table_repl",
                "replica_name": "{replica}",
            },
            id="ReplicatedMergeTree",
        ),
        pytest.param(
            "metadata/table_replicated_merge_tree_ver.sql",
            {
                "table_uuid": "f438d816-605d-4fe0-a9cb-4edba3ce72dd",
                "table_engine": MergeTreeFamilyEngines.REPLICATED_MERGE_TREE,
                "replica_path": "/clickhouse/tables/{shard}/test_table_repl1",
                "replica_name": "{replica}",
            },
            id="ReplicatedMergeTree with ver",
        ),
        pytest.param(
            "metadata/table_replicated_replacing_merge_tree.sql",
            {
                "table_uuid": "4ce817e2-8043-4655-869e-eeab3edeae6a",
                "table_engine": MergeTreeFamilyEngines.REPLICATED_REPLACING_MERGE_TREE,
                "replica_path": "/clickhouse/tables/tableName/{shard}/",
                "replica_name": "{replica}",
            },
            id="ReplicatedReplacingMergeTree",
        ),
        pytest.param(
            "metadata/table_replicated_summing_merge_tree.sql",
            {
                "table_uuid": "72b4c520-9cc2-4549-ba6c-bd952bb049d8",
                "table_engine": MergeTreeFamilyEngines.REPLICATED_SUMMING_MERGE_TREE,
                "replica_path": "/clickhouse/tables/tableName/{shard}/1",
                "replica_name": "{replica}",
            },
            id="ReplicatedSummingMergeTree",
        ),
        pytest.param(
            "metadata/table_replicated_aggregating_merge_tree.sql",
            {
                "table_uuid": "8ac44a5e-091e-4dc4-9eb0-0ba577b3afd7",
                "table_engine": MergeTreeFamilyEngines.REPLICATED_AGGREGATING_MERGE_TREE,
                "replica_path": "/clickhouse/tables/{shard}/example_replicated_aggregating_mergetree",
                "replica_name": "{replica}",
            },
            id="ReplicatedAggregatingMergeTree",
        ),
        pytest.param(
            "metadata/table_replicated_collapsing_merge_tree.sql",
            {
                "table_uuid": "9317cb30-1efd-44bd-ab88-d0e3a025965a",
                "table_engine": MergeTreeFamilyEngines.REPLICATED_COLLAPSING_MERGE_TREE,
                "replica_path": "/clickhouse/tables/{shard}/example_replicated_collapsing_mergetree",
                "replica_name": "{replica}",
            },
            id="ReplicatedCollapsingMergeTree",
        ),
        pytest.param(
            "metadata/table_replicated_versioned_collapsing_merge_tree.sql",
            {
                "table_uuid": "10ccbec1-6b78-48fe-a51a-fb7c9f7fbe4a",
                "table_engine": MergeTreeFamilyEngines.REPLICATED_VERSIONED_MERGE_TREE,
                "replica_path": "/clickhouse/tables/{shard}/example_replicated_versioned_collapsing_mergetree",
                "replica_name": "{replica}",
            },
            id="ReplicatedVersionedCollapsingMergeTree",
        ),
    ],
)
def test_parse_table_metadata_repl(file, expected):
    metadata = parse_table_metadata(table_metadata_path=PATH_TO_TESTS + file)
    assert metadata.table_uuid == expected["table_uuid"]
    assert metadata.table_engine == expected["table_engine"]
    assert metadata.replica_path == expected["replica_path"]
    assert metadata.replica_name == expected["replica_name"]


@pytest.mark.parametrize(
    "file,exception_msg",
    [
        pytest.param(
            "metadata/broken_uuid.sql",
            "Failed parse UUID from metadata",
            id="Broken UUID",
        ),
        pytest.param(
            "metadata/broken_no_uuid.sql",
            "Failed parse UUID from metadata.",
            id="No UUID",
        ),
        pytest.param(
            "metadata/broken_no_uuid_full.sql",
            f"Empty UUID from metadata: '{PATH_TO_TESTS}metadata/broken_no_uuid_full.sql'",
            id="No UUID full",
        ),
        pytest.param(
            "metadata/broken_no_engine.sql",
            f"Empty table engine from metadata: '{PATH_TO_TESTS}metadata/broken_no_engine.sql'",
            id="No engine",
        ),
        pytest.param(
            "metadata/broken_no_engine_full.sql",
            f"Empty table engine from metadata: '{PATH_TO_TESTS}metadata/broken_no_engine_full.sql'",
            id="No engine full",
        ),
    ],
)
def test_broken_metadata(file, exception_msg):
    with pytest.raises(RuntimeError, match=exception_msg):
        _ = parse_table_metadata(table_metadata_path=PATH_TO_TESTS + file)


@pytest.mark.parametrize(
    "start,finish,is_replicated",
    [
        pytest.param(
            None,
            MergeTreeFamilyEngines.REPLICATED_MERGE_TREE,
            False,
            id="Non replicated",
        ),
        pytest.param(
            MergeTreeFamilyEngines.REPLICATED_MERGE_TREE,
            None,
            True,
            id="Replicated",
        ),
    ],
)
def test_is_engine_replicated(start, finish, is_replicated):
    engines_list = list(MergeTreeFamilyEngines)
    start_idx = 0 if start is None else engines_list.index(start)
    finish_idx = len(engines_list) if finish is None else engines_list.index(finish)

    for idx in range(start_idx, finish_idx):
        assert is_replicated == engines_list[idx].is_table_engine_replicated()


def test_last_merge_tree_family_engine():
    engines_list = list(MergeTreeFamilyEngines)
    assert MergeTreeFamilyEngines.REPLICATED_GRAPHITE_MERGE_TREE == engines_list[-1]


@pytest.mark.parametrize(
    "path,result",
    [
        pytest.param(
            "/clickhouse/foo/{uuid}",
            True,
            id="with uuid",
        ),
        pytest.param(
            "/clickhouse/foo/uuid_something",
            False,
            id="with similar uuid",
        ),
        pytest.param(
            "/clickhouse/foo/something",
            False,
            id="wo uuid",
        ),
    ],
)
def test_replicated_path_contains_uuid_macros(path, result):
    assert result == check_replica_path_contains_macros(path=path, macros="uuid")
