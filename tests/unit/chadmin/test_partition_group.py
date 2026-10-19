from typing import Any
from unittest.mock import ANY, MagicMock, call, patch

from click.testing import CliRunner

from ch_tools.chadmin.cli.partition_group import (
    _attach_operation_report,
    get_partitions,
    partition_group,
)
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


def test_detached_partitions_are_filtered_by_reason() -> None:
    query = render_partitions_query(detached=True, reason="broken-on-start")

    assert "reason LIKE 'broken-on-start'" in query


def test_cleanup_old_zero_copy_locks_requires_reason() -> None:
    result = CliRunner().invoke(
        partition_group,
        ["attach", "--all", "--cleanup-old-zero-copy-locks"],
        obj={"config": {"loguru": {"handlers": {}, "formatters": {}}}},
    )

    assert result.exit_code == 2
    assert (
        "when --cleanup-old-zero-copy-locks is set, --reason is required"
        in result.output
    )


def test_checked_attach_rejects_unknown_reason() -> None:
    result = CliRunner().invoke(
        partition_group,
        ["attach", "--all", "--reason", "broken_on_start"],
    )

    assert result.exit_code == 2
    assert "Invalid value for '--reason'" in result.output


def test_attach_help_groups_s3_checked_options() -> None:
    result = CliRunner().invoke(partition_group, ["attach", "--help"])

    assert result.exit_code == 0, result.output
    assert "S3 checked attach options:" in result.output
    assert "--quarantine-failed-unprefixed" in result.output
    assert "--verbose" not in result.output


def test_attach_operation_report_only_includes_renames_and_partition_attaches() -> None:
    report = [
        {
            "database": "db",
            "table": "table",
            "partition_id": "first",
            "part": "1_1_1_0",
            "reason": "broken-on-start",
            "path": "/var/lib/clickhouse/detached/broken-on-start_1_1_1_0",
            "action": "attached",
        },
        {
            "database": "db",
            "table": "table",
            "partition_id": "first",
            "part": "1_2_2_0",
            "reason": "",
            "path": "/var/lib/clickhouse/detached/1_2_2_0",
            "action": "attached",
        },
        {
            "database": "db",
            "table": "table",
            "partition_id": "first",
            "part": "1_3_3_0",
            "reason": "broken-on-start",
            "path": "/var/lib/clickhouse/detached/broken-on-start_1_3_3_0",
            "action": "left-prefixed",
        },
        {
            "database": "db",
            "table": "table",
            "partition_id": "first",
            "part": "1_4_4_0",
            "reason": "",
            "path": "/var/lib/clickhouse/detached/1_4_4_0",
            "destination_part": "broken_1_4_4_0",
            "action": "quarantined",
        },
    ]

    assert _attach_operation_report(report) == [
        {
            "action": "quarantine",
            "database": "db",
            "table": "table",
            "partition_id": "first",
            "part": "1_4_4_0",
            "destination_part": "broken_1_4_4_0",
        },
        {
            "action": "rename",
            "database": "db",
            "table": "table",
            "partition_id": "first",
            "part": "broken-on-start_1_1_1_0",
            "destination_part": "1_1_1_0",
        },
        {
            "action": "attach-partition",
            "database": "db",
            "table": "table",
            "partition_id": "first",
            "part": "",
            "destination_part": "",
        },
    ]


def test_checked_attach_prints_operations() -> None:
    report = [
        {
            "database": "db",
            "table": "table",
            "partition_id": "first",
            "part": "1_1_1_0",
            "reason": "broken-on-start",
            "path": "/var/lib/clickhouse/detached/broken-on-start_1_1_1_0",
            "action": "attached",
        }
    ]
    config: dict[str, Any] = {"loguru": {"handlers": {}, "formatters": {}}}

    with (
        patch(
            "ch_tools.chadmin.cli.partition_group.get_partitions",
            return_value={"data": []},
        ),
        patch(
            "ch_tools.chadmin.cli.partition_group.attach_checked_partitions",
            return_value=report,
        ) as attach_checked,
        patch("ch_tools.chadmin.cli.partition_group.echo") as echo,
    ):
        result = CliRunner().invoke(
            partition_group,
            ["attach", "--all", "--reason", "broken-on-start"],
            obj={"config": config},
        )

    assert result.exit_code == 0, result.output
    attach_checked.assert_called_once_with(
        ANY,
        [],
        reason="broken-on-start",
        check_method=None,
        dry_run=False,
        keep_going=False,
        cleanup_old_zero_copy_locks=False,
        quarantine_failed_unprefixed=None,
    )
    echo.assert_has_calls(
        [
            call("Move db.table: broken-on-start_1_1_1_0 -> 1_1_1_0"),
            call("Attach partition db.table: first"),
        ]
    )
    assert echo.call_count == 2


def test_checked_attach_fails_on_failed_s3_check() -> None:
    report = [
        {
            "database": "db",
            "table": "table",
            "partition_id": "first",
            "part": "1_1_1_0",
            "reason": "broken-on-start",
            "path": "/var/lib/clickhouse/detached/broken-on-start_1_1_1_0",
            "check": "failed",
            "action": "left-prefixed",
        }
    ]
    config: dict[str, Any] = {"loguru": {"handlers": {}, "formatters": {}}}

    with (
        patch(
            "ch_tools.chadmin.cli.partition_group.get_partitions",
            return_value={"data": []},
        ),
        patch(
            "ch_tools.chadmin.cli.partition_group.attach_checked_partitions",
            return_value=report,
        ),
    ):
        result = CliRunner().invoke(
            partition_group,
            ["attach", "--all", "--reason", "broken-on-start"],
            obj={"config": config},
        )

    assert result.exit_code == 1
    assert (
        "S3 check failed for 1 detached part(s); "
        "the failed parts were not attached" in result.output
    )


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
