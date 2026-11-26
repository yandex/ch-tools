from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from ch_tools.chadmin.internal.part import list_detached_parts
from ch_tools.common.clickhouse.client.clickhouse_client import ClickhouseClient


def render_query(version: str = "23.9.1.1854", **kwargs: Any) -> str:
    ctx = MagicMock()
    ctx.obj = {"config": {"clickhouse": {"version": version}}}
    with patch(
        "ch_tools.chadmin.internal.part.execute_query", return_value={"data": []}
    ) as execute:
        list_detached_parts(ctx, **kwargs)
    client = MagicMock()
    client.get_clickhouse_version.return_value = version
    query = ClickhouseClient.render_query(
        client, execute.call_args.args[1], **execute.call_args.kwargs
    )
    return " ".join(query.split())


@pytest.mark.parametrize("version", ["22.8.1.1", "23.8.1.2992", "23.9.1.1854"])
def test_no_age_filter_preserves_existing_calls(version: str) -> None:
    query = render_query(version)
    assert "AND (" not in query
    assert "modification_time" not in query
    assert "disks_count" not in query
    assert "OVER" not in query


@pytest.mark.parametrize("ages", [{}, {"non_empty_reason_min_age": 7200}])
def test_size_is_included_by_default(ages: dict) -> None:
    query = render_query(**ages)
    assert query == render_query(include_size=True, **ages)
    assert query.count("bytes_on_disk") == (2 if ages else 1)


@pytest.mark.parametrize("ages", [{}, {"non_empty_reason_min_age": 7200}])
def test_size_can_be_omitted_from_every_select(ages: dict) -> None:
    query = render_query(include_size=False, **ages)
    assert "bytes_on_disk" not in query
    assert "path" in query
    assert query.count("FROM system.detached_parts") == 1
    if ages:
        assert "disks_count" in query
        assert "modification_time < now() - INTERVAL 7200 SECOND" in query


@pytest.mark.parametrize("include_size", [False, True])
def test_size_flag_preserves_older_clickhouse_columns(include_size: bool) -> None:
    query = render_query("22.8.1.1", include_size=include_size)
    assert "bytes_on_disk" not in query
    assert "path" not in query


@pytest.mark.parametrize("empty,non_empty", [(0, 0), (0, None), (None, 0)])
def test_disabled_categories_select_no_parts(empty: Any, non_empty: Any) -> None:
    query = render_query(empty_reason_min_age=empty, non_empty_reason_min_age=non_empty)
    assert "AND ( 0 )" in query
    assert "modification_time <" not in query


def test_both_categories_have_independent_age_thresholds() -> None:
    query = render_query(empty_reason_min_age=3600, non_empty_reason_min_age=7200)
    assert "reason = '' AND modification_time < now() - INTERVAL 3600 SECOND" in query
    assert "reason != '' AND modification_time < now() - INTERVAL 7200 SECOND" in query
    assert "AND ( 0 OR" in query


@pytest.mark.parametrize("empty", [None, 0])
def test_non_empty_reason_filter_excludes_disabled_empty_reason(empty: Any) -> None:
    query = render_query(empty_reason_min_age=empty, non_empty_reason_min_age=7200)
    assert "reason != ''" in query
    assert "reason = ''" not in query


def test_empty_reason_filter_excludes_non_empty_reason() -> None:
    query = render_query(empty_reason_min_age=3600)
    assert "reason = ''" in query
    assert "reason != ''" not in query


def test_age_filter_combines_with_existing_filters_before_limit() -> None:
    query = render_query(
        database="db",
        table="test",
        reason="broken",
        limit=10,
        non_empty_reason_min_age=7200,
    )
    assert "WHERE database LIKE 'db'" in query
    assert "AND table LIKE 'test'" in query
    assert "AND reason LIKE 'broken'" in query
    assert query.index("modification_time") < query.index("LIMIT 10")


def test_disk_count_includes_copies_excluded_by_selection_filters() -> None:
    query = render_query(
        non_empty_reason_min_age=7200,
        disk_name="cold",
        reason="broken",
        level=1,
        limit=10,
    )
    assert query.count("FROM system.detached_parts") == 1
    assert (
        "uniqExact(disk) OVER ( PARTITION BY database, table, name ) AS disks_count"
        in query
    )
    # The window sees fresh copies and copies on other disks before any
    # selection filter or LIMIT can hide them.
    inner, outer = query.split("FROM system.detached_parts", maxsplit=1)
    assert "WHERE" not in inner
    assert "WHERE" in outer
    assert "AND disk LIKE 'cold'" in outer
    assert "AND reason LIKE 'broken'" in outer
    assert "AND level = 1" in outer
    assert "modification_time < now() - INTERVAL 7200 SECOND" in outer
    assert "LIMIT 10" in outer


@pytest.mark.parametrize(
    "ages",
    [
        {"empty_reason_min_age": 1},
        {"non_empty_reason_min_age": 1},
        {"empty_reason_min_age": 0},
    ],
)
def test_old_version_rejects_age_filter_before_query(ages: dict) -> None:
    ctx = MagicMock()
    ctx.obj = {"config": {"clickhouse": {"version": "23.8.1.2992"}}}
    with patch("ch_tools.chadmin.internal.part.execute_query") as execute:
        with pytest.raises(RuntimeError, match="23.9"):
            list_detached_parts(ctx, **ages)
    execute.assert_not_called()


def test_json_list_without_age_filter_still_works() -> None:
    parts = [{"database": "db", "table": "test", "name": "1_1_1_0"}]
    with patch(
        "ch_tools.chadmin.internal.part.read_and_validate_parts_from_json",
        return_value={"data": parts},
    ):
        assert (
            list_detached_parts(MagicMock(), use_part_list_from_json="parts.json")
            == parts
        )
