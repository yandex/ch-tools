from typing import Any
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from ch_tools.chadmin.cli.detached_parts_cleanup_command import (
    detached_parts_cleanup_command,
)


@pytest.fixture
def operations(monkeypatch: pytest.MonkeyPatch) -> dict:
    prefix = "ch_tools.chadmin.cli.detached_parts_cleanup_command."
    mocks = {
        "list_detached_parts": MagicMock(return_value=[]),
        "execute_query": MagicMock(return_value={"data": []}),
        "get_disks": MagicMock(
            return_value={"local": {"name": "local", "path": "/data/"}}
        ),
        "get_version": MagicMock(return_value="25.8.1"),
        "drop_detached_part": MagicMock(),
        "remove_from_ch_disk": MagicMock(return_value=(0, b"")),
        "logging.warning": MagicMock(),
        "logging.error": MagicMock(),
        "logging.info": MagicMock(),
    }
    for name, mock in mocks.items():
        monkeypatch.setattr(prefix + name, mock)
    return mocks


def invoke(dry_run: bool = False, automatic: int = 7200, manual: int = 3600) -> Any:
    return CliRunner().invoke(
        detached_parts_cleanup_command,
        ["--dry-run"] if dry_run else [],
        obj={},
        default_map={
            "automatically_detached_data_parts_retention_time": automatic,
            "manually_detached_data_parts_retention_time": manual,
        },
    )


def make_part(name: str = "1_1_1_0", **kwargs: Any) -> dict:
    return {
        "database": "db",
        "table": "test",
        "name": name,
        "disk_name": "local",
        "path": f"/data/detached/{name}",
        **kwargs,
    }


@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize("suffix", [False, True])
def test_cleanup_uses_configured_ages_and_existing_deletion_backend(
    operations: dict, dry_run: bool, suffix: bool
) -> None:
    part = make_part("broken_1_1_1_0_try1" if suffix else "1_1_1_0")
    operations["list_detached_parts"].return_value = [part]
    result = invoke(dry_run)
    assert result.exit_code == 0, result.exception
    selection = operations["list_detached_parts"].call_args
    assert selection.kwargs == {
        "empty_reason_min_age": 3600,
        "non_empty_reason_min_age": 7200,
    }
    if suffix:
        operations["remove_from_ch_disk"].assert_called_once_with(
            "local", f"detached/{part['name']}", "25.8.1", dry_run=dry_run
        )
        operations["drop_detached_part"].assert_not_called()
    else:
        operations["drop_detached_part"].assert_called_once_with(
            selection.args[0], "db", "test", "1_1_1_0", dry_run=dry_run
        )
        operations["remove_from_ch_disk"].assert_not_called()


def test_empty_selection_does_not_read_disks_or_delete(operations: dict) -> None:
    assert invoke().exit_code == 0
    operations["execute_query"].assert_not_called()
    operations["get_disks"].assert_not_called()
    operations["drop_detached_part"].assert_not_called()
    operations["remove_from_ch_disk"].assert_not_called()


def test_disabled_cleanup_does_not_query_clickhouse(operations: dict) -> None:
    assert invoke(automatic=0, manual=0).exit_code == 0
    operations["list_detached_parts"].assert_not_called()
    operations["execute_query"].assert_not_called()


def test_deletion_error_does_not_block_remaining_parts(operations: dict) -> None:
    parts = [make_part(), make_part("2_2_2_0")]
    operations["list_detached_parts"].return_value = parts
    operations["drop_detached_part"].side_effect = [RuntimeError("drop failed"), None]
    result = invoke()
    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)
    assert "Failed to delete 1 detached data part(s)" in result.output
    assert operations["drop_detached_part"].call_count == 2
    operations["logging.error"].assert_called_once()
    assert operations["logging.error"].call_args.args[1] == parts[0]
    operations["logging.info"].assert_called_with(
        "Detached parts cleanup{}: processed={}, skipped={}, failed={}", "", 1, 0, 1
    )


@pytest.mark.parametrize("name", ["attaching_1_1_1_0", "deleting_1_1_1_0_try1"])
def test_service_parts_are_skipped(operations: dict, name: str) -> None:
    operations["list_detached_parts"].return_value = [make_part(name)]
    assert invoke().exit_code == 0
    operations["get_disks"].assert_not_called()
    operations["drop_detached_part"].assert_not_called()
    operations["remove_from_ch_disk"].assert_not_called()
    operations["logging.warning"].assert_called_once()


def test_fresh_copy_on_another_disk_prevents_deletion(operations: dict) -> None:
    # Only the old copy is selected by age; the second query must also see
    # the fresh copy on the other disk, which has the same SQL identity.
    old_copy = make_part(disk_name="cold", path="/cold/detached/1_1_1_0")
    operations["list_detached_parts"].return_value = [old_copy]
    operations["execute_query"].return_value = {
        "data": [{"database": "db", "table": "test", "name": old_copy["name"]}]
    }
    assert invoke().exit_code == 0
    operations["drop_detached_part"].assert_not_called()
    operations["remove_from_ch_disk"].assert_not_called()
    operations["get_disks"].assert_not_called()
    operations["logging.warning"].assert_called_once()
    query = " ".join(operations["execute_query"].call_args.args[1].split())
    assert "FROM system.detached_parts GROUP BY database, table, name" in query
    assert "HAVING uniqExact(disk) > 1" in query
    assert "WHERE" not in query


@pytest.mark.parametrize("other", [{"database": "other_db"}, {"table": "other_table"}])
def test_ambiguity_is_scoped_to_database_and_table(
    operations: dict, other: dict
) -> None:
    part = make_part()
    operations["list_detached_parts"].return_value = [part]
    operations["execute_query"].return_value = {"data": [make_part(**other)]}
    assert invoke().exit_code == 0
    operations["drop_detached_part"].assert_called_once()


def test_disk_failure_is_reported_and_remaining_parts_are_processed(
    operations: dict,
) -> None:
    part = make_part("broken_1_1_1_0_try1")
    operations["list_detached_parts"].return_value = [part, make_part("2_2_2_0")]
    operations["remove_from_ch_disk"].return_value = (1, b"permission denied")
    result = invoke()
    assert result.exit_code == 1
    operations["drop_detached_part"].assert_called_once()
    operations["logging.error"].assert_called_once()
    error = operations["logging.error"].call_args.args[2]
    assert "code 1: permission denied" in str(error)


def test_disk_path_outside_its_disk_is_not_removed(operations: dict) -> None:
    operations["list_detached_parts"].return_value = [
        make_part("broken_1_1_1_0_try1", path="/other/detached/part")
    ]
    assert invoke().exit_code == 1
    operations["remove_from_ch_disk"].assert_not_called()


@pytest.mark.parametrize("query", ["list_detached_parts", "execute_query"])
def test_failed_inventory_prevents_all_deletions(operations: dict, query: str) -> None:
    operations["list_detached_parts"].return_value = [make_part()]
    operations[query].side_effect = RuntimeError("inventory failed")
    assert invoke().exit_code != 0
    operations["drop_detached_part"].assert_not_called()
    operations["remove_from_ch_disk"].assert_not_called()


@pytest.mark.parametrize("keep_going", [False, True])
def test_manual_delete_uses_helper_and_preserves_error_handling(
    monkeypatch: pytest.MonkeyPatch, keep_going: bool
) -> None:
    from ch_tools.chadmin.cli.part_group import part_group
    from ch_tools.common.config import DEFAULT_CONFIG

    parts = [
        {"database": "db", "table": "test", "name": "1_1_1_0"},
        {"database": "db", "table": "test", "name": "2_2_2_0"},
    ]
    disks = {"local": {"name": "local", "path": "/data/"}}
    delete = MagicMock(side_effect=[RuntimeError("drop failed"), None])
    monkeypatch.setattr(
        "ch_tools.chadmin.cli.part_group.list_detached_parts",
        MagicMock(return_value=parts),
    )
    monkeypatch.setattr(
        "ch_tools.chadmin.cli.part_group.get_disks", MagicMock(return_value=disks)
    )
    monkeypatch.setattr("ch_tools.chadmin.cli.part_group.delete_detached_part", delete)
    monkeypatch.setattr("ch_tools.common.logging.configure", MagicMock())
    monkeypatch.setattr("ch_tools.common.logging._log", MagicMock())
    args = ["delete", "--detached", "--all", "--dry-run"]
    if keep_going:
        args.append("--keep-going")
    result = CliRunner().invoke(part_group, args, obj={"config": DEFAULT_CONFIG})
    assert result.exit_code == (0 if keep_going else 1), result.exception
    assert delete.call_count == (2 if keep_going else 1)
    assert delete.call_args.args[1:] == (parts[-1] if keep_going else parts[0], disks)
    assert delete.call_args.kwargs == {"dry_run": True}
