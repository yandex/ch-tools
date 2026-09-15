from typing import Any
from unittest.mock import MagicMock, patch

from ch_tools.chadmin.cli.partition_group import get_partitions
from ch_tools.common.clickhouse.client.clickhouse_client import ClickhouseClient


def render_partitions_query(ch_version: str = "24.8.1.1", **kwargs: Any) -> str:
    """
    Render the query that get_partitions would have executed.
    """
    with patch("ch_tools.chadmin.cli.partition_group.execute_query") as execute_query:
        get_partitions(MagicMock(), None, None, **kwargs)

    _, query = execute_query.call_args.args
    client = MagicMock()
    client.get_clickhouse_version.return_value = ch_version
    return ClickhouseClient.render_query(
        client, query, **execute_query.call_args.kwargs
    )


def test_detached_partitions_are_filtered_by_disk() -> None:
    query = render_partitions_query(detached=True, disk_name="object_storage")

    assert "has(groupUniqArray(disk), 'object_storage')" in query
    # The filter is an aggregate, so it must come after GROUP BY, not in WHERE.
    assert query.index("GROUP BY") < query.index("groupUniqArray(disk)")


def test_detached_partitions_are_not_filtered_by_disk_by_default() -> None:
    query = render_partitions_query(detached=True)

    assert "groupUniqArray" not in query


def test_partitions_are_filtered_by_part_count() -> None:
    query = render_partitions_query(min_part_count=5, max_part_count=10)

    assert "parts >= 5" in query
    assert "parts <= 10" in query


def test_detached_partitions_are_filtered_by_size() -> None:
    query = render_partitions_query(detached=True, min_size=1024, max_size=4096)

    assert "sum(bytes_on_disk) >= 1024" in query
    assert "sum(bytes_on_disk) <= 4096" in query


def test_detached_partitions_are_not_filtered_by_size_on_old_clickhouse() -> None:
    # system.detached_parts has no bytes_on_disk column before 23.1.
    query = render_partitions_query(
        ch_version="22.8.1.1", detached=True, min_size=1024, max_size=4096
    )

    assert "bytes_on_disk" not in query
