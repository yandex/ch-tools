import pytest

from ch_tools.chadmin.cli.table_metadata import TableMetadata


@pytest.mark.parametrize(
    "file,expected",
    [
        pytest.param(
            "metadata/table_merge_tree.sql",
            {
                "table_uuid": "9612256b-b461-4df5-8015-72f9727d1f95",
                "table_engine": "MergeTree",
            },
            id="MergeTree",
        ),
        pytest.param(
            "metadata/table_replacing_merge_tree.sql",
            {
                "table_uuid": "c322e832-2628-45f9-b2f5-fd659078c5c2",
                "table_engine": "ReplacingMergeTree",
            },
            id="ReplacingMergeTree",
        ),
        pytest.param(
            "metadata/table_summing_merge_tree.sql",
            {
                "table_uuid": "5f55555d-c9f7-47ac-8d87-b3ac8a889161",
                "table_engine": "SummingMergeTree",
            },
            id="SummingMergeTree",
        ),
        pytest.param(
            "metadata/table_aggregating_merge_tree.sql",
            {
                "table_uuid": "40c7c9a8-7451-4a10-8b43-443436f33413",
                "table_engine": "AggregatingMergeTree",
            },
            id="AggregatingMergeTree",
        ),
        pytest.param(
            "metadata/table_collapsing_merge_tree.sql",
            {
                "table_uuid": "122b369e-3866-4c2b-8ca1-9e07c75ecee0",
                "table_engine": "CollapsingMergeTree",
            },
            id="CollapsingMergeTree",
        ),
        pytest.param(
            "metadata/table_versioned_collapsing_merge_tree.sql",
            {
                "table_uuid": "42089e02-c13c-4b52-a1bf-4f9aa3e84e56",
                "table_engine": "VersionedCollapsingMergeTree",
            },
            id="VersionedCollapsingMergeTree",
        ),
        pytest.param(
            "metadata/table_replicated_merge_tree.sql",
            {
                "table_uuid": "f438d816-605d-4fe0-a9cb-4edba3ce72dd",
                "table_engine": "ReplicatedMergeTree",
            },
            id="ReplicatedMergeTree",
        ),
    ],
)
def test_metadata_parser(file, expected):
    metadata = TableMetadata(table_metadata_path=file)
    assert metadata.table_uuid == expected["table_uuid"]
    assert metadata.table_engine == expected["table_engine"]


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
            "Empty UUID from metadata: 'metadata/broken_no_uuid_full.sql'",
            id="No UUID full",
        ),
        pytest.param(
            "metadata/broken_no_engine.sql",
            "Failed parse ENGINE",
            id="No engine",
        ),
        pytest.param(
            "metadata/broken_no_engine_full.sql",
            "Empty table engine from metadata: 'metadata/broken_no_engine_full.sql'",
            id="No engine full",
        ),
    ],
)
def test_broken_metadata(file, exception_msg):
    with pytest.raises(RuntimeError, match=exception_msg):
        _ = TableMetadata(table_metadata_path=file)
