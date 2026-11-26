import fcntl
import select
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import requests
from click.testing import CliRunner

from ch_tools.chadmin.cli.detached_parts_cleanup_command import (
    detached_parts_cleanup_command,
)
from ch_tools.chadmin.internal.part import drop_detached_part
from ch_tools.common.clickhouse.client.clickhouse_client import ClickhouseClient


@pytest.fixture
def operations(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> dict:
    prefix = "ch_tools.chadmin.cli.detached_parts_cleanup_command."
    mocks: dict[str, Any] = {
        "list_detached_parts": MagicMock(return_value=[]),
        "drop_detached_part": MagicMock(),
        "logging.warning": MagicMock(),
        "logging.error": MagicMock(),
        "logging.info": MagicMock(),
    }
    for name, mock in mocks.items():
        monkeypatch.setattr(prefix + name, mock)
    mocks["lock_file"] = tmp_path / "cleanup.lock"
    return mocks


def invoke(
    operations: dict, dry_run: bool = False, automatic: int = 7200, manual: int = 3600
) -> Any:
    return CliRunner().invoke(
        detached_parts_cleanup_command,
        ["--lock-file", str(operations["lock_file"])]
        + (["--dry-run"] if dry_run else []),
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
        "disks_count": 1,
        **kwargs,
    }


@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize(
    "name", ["1_1_1_0", "broken_1_1_1_0_try1", "broken_1_1_1_0_try10"]
)
def test_cleanup_uses_sql_drop_for_all_names(
    operations: dict, dry_run: bool, name: str
) -> None:
    part = make_part(name)
    operations["list_detached_parts"].return_value = [part]
    result = invoke(operations, dry_run)
    assert result.exit_code == 0, result.exception
    selection = operations["list_detached_parts"].call_args
    operations["list_detached_parts"].assert_called_once()
    assert selection.kwargs == {
        "empty_reason_min_age": 3600,
        "non_empty_reason_min_age": 7200,
        "include_size": False,
    }
    operations["drop_detached_part"].assert_called_once_with(
        selection.args[0],
        "db",
        "test",
        name,
        dry_run=dry_run,
        retry_max_attempts=1,
    )


def test_empty_selection_does_not_delete(operations: dict) -> None:
    assert invoke(operations).exit_code == 0
    operations["drop_detached_part"].assert_not_called()


def test_cleanup_does_not_require_disk_or_path_fields(operations: dict) -> None:
    operations["list_detached_parts"].return_value = [
        {"database": "db", "table": "test", "name": "1_1_1_0", "disks_count": 1}
    ]
    assert invoke(operations).exit_code == 0
    operations["drop_detached_part"].assert_called_once()


def test_disabled_cleanup_does_not_query_clickhouse(operations: dict) -> None:
    assert invoke(operations, automatic=0, manual=0).exit_code == 0
    operations["list_detached_parts"].assert_not_called()
    assert not operations["lock_file"].exists()


@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize("kill_holder", [False, True])
def test_overlapping_process_skips_work_and_releases_lock(
    operations: dict, dry_run: bool, kill_holder: bool
) -> None:
    holder_code = """
import fcntl
import sys

with open(sys.argv[1], 'a', encoding='utf-8') as handle:
    fcntl.flock(handle, fcntl.LOCK_EX)
    print('locked', flush=True)
    sys.stdin.read()
"""
    holder = subprocess.Popen(
        [sys.executable, "-c", holder_code, str(operations["lock_file"])],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout is not None
        ready, _, _ = select.select([holder.stdout], [], [], 5)
        assert ready, "Lock holder did not become ready"
        assert holder.stdout.readline() == "locked\n"
        inode = operations["lock_file"].stat().st_ino

        result = invoke(operations, dry_run=dry_run)
        assert result.exit_code == 0, result.exception
        operations["list_detached_parts"].assert_not_called()
        operations["drop_detached_part"].assert_not_called()
        operations["logging.info"].assert_called_once_with(
            "Detached parts cleanup is already running; skipping"
        )
    finally:
        if kill_holder:
            holder.kill()
        holder.communicate(timeout=5)

    # No stale lock survives normal exit or a killed process. The file is
    # retained so contenders always lock the same inode.
    assert invoke(operations).exit_code == 0
    operations["list_detached_parts"].assert_called_once()
    assert operations["lock_file"].stat().st_ino == inode


@pytest.mark.parametrize("failure_stage", [None, "inventory", "delete"])
def test_lock_covers_inventory_and_deletion_and_is_released(
    operations: dict, failure_stage: str | None
) -> None:
    def assert_locked() -> None:
        with open(operations["lock_file"], "a", encoding="utf-8") as contender:
            with pytest.raises(BlockingIOError):
                fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def inventory(*_args: Any, **_kwargs: Any) -> list[dict]:
        assert_locked()
        if failure_stage == "inventory":
            raise RuntimeError("Inventory failed")
        return [make_part()]

    def delete(*_args: Any, **_kwargs: Any) -> None:
        assert_locked()
        if failure_stage == "delete":
            raise RuntimeError("Deletion failed")

    operations["list_detached_parts"].side_effect = inventory
    operations["drop_detached_part"].side_effect = delete
    result = invoke(operations)
    assert result.exit_code == (0 if failure_stage is None else 1)
    with open(operations["lock_file"], "a", encoding="utf-8") as contender:
        fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_lock_open_error_prevents_cleanup(operations: dict) -> None:
    operations["lock_file"] = (
        operations["lock_file"].parent / "missing" / "cleanup.lock"
    )
    assert invoke(operations).exit_code != 0
    operations["list_detached_parts"].assert_not_called()
    operations["drop_detached_part"].assert_not_called()


def test_deletion_error_does_not_block_remaining_parts(operations: dict) -> None:
    parts = [make_part(), make_part("2_2_2_0")]
    operations["list_detached_parts"].return_value = parts
    operations["drop_detached_part"].side_effect = [RuntimeError("drop failed"), None]
    result = invoke(operations)
    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)
    assert "Failed to delete 1 detached data part(s)" in result.output
    assert operations["drop_detached_part"].call_count == 2
    operations["logging.error"].assert_called_once()
    assert operations["logging.error"].call_args.args[1] == parts[0]
    operations["logging.info"].assert_called_with(
        "Detached parts cleanup{}: processed={}, skipped={}, failed={}", "", 1, 0, 1
    )


def test_lost_drop_response_does_not_delete_replacement_part(
    operations: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    parts = [make_part(), make_part("2_2_2_0")]
    operations["list_detached_parts"].return_value = parts
    monkeypatch.setattr(
        "ch_tools.chadmin.cli.detached_parts_cleanup_command.drop_detached_part",
        drop_detached_part,
    )
    client = ClickhouseClient(
        host="localhost",
        ports={},
        timeout=1,
        retry_max_attempts=3,
        retry_max_interval=0,
    )
    monkeypatch.setattr(
        "ch_tools.common.clickhouse.client.clickhouse_client._log_retry_attempt",
        MagicMock(),
    )
    server_state = {"replacement_exists": False}

    def execute(query: str, **kwargs: Any) -> None:
        assert "retry_max_attempts" not in kwargs["query_args"]
        if "'1_1_1_0'" in query:
            if server_state["replacement_exists"]:
                server_state["replacement_exists"] = False
                return
            # The first DROP succeeded, but its response was lost. Another
            # actor immediately detaches a fresh part under the same name.
            server_state["replacement_exists"] = True
            raise requests.exceptions.ReadTimeout("DROP response lost")

    execute_mock = MagicMock(side_effect=execute)
    monkeypatch.setattr(client, "_execute_query_impl", execute_mock)
    result = CliRunner().invoke(
        detached_parts_cleanup_command,
        ["--lock-file", str(operations["lock_file"])],
        obj={
            "config": {"clickhouse": {"alter_table_timeout": 10}},
            "chcli": client,
        },
        default_map={
            "automatically_detached_data_parts_retention_time": 7200,
            "manually_detached_data_parts_retention_time": 3600,
        },
    )
    assert result.exit_code == 1
    assert server_state["replacement_exists"]
    assert execute_mock.call_count == 2
    assert "'2_2_2_0'" in execute_mock.call_args.kwargs["query"]
    operations["logging.info"].assert_called_with(
        "Detached parts cleanup{}: processed={}, skipped={}, failed={}", "", 1, 0, 1
    )


@pytest.mark.parametrize("name", ["attaching_1_1_1_0", "deleting_1_1_1_0_try1"])
def test_service_parts_are_skipped(operations: dict, name: str) -> None:
    operations["list_detached_parts"].return_value = [make_part(name)]
    assert invoke(operations).exit_code == 0
    operations["drop_detached_part"].assert_not_called()
    operations["logging.warning"].assert_called_once()


def test_fresh_copy_on_another_disk_prevents_deletion(operations: dict) -> None:
    # The single query only returns the old copy, but disks_count also
    # includes the fresh copy on the other disk.
    old_copy = make_part(disk_name="cold", path="/cold/detached/1_1_1_0", disks_count=2)
    operations["list_detached_parts"].return_value = [old_copy]
    assert invoke(operations).exit_code == 0
    operations["drop_detached_part"].assert_not_called()
    operations["logging.warning"].assert_called_once()
    operations["list_detached_parts"].assert_called_once()


@pytest.mark.parametrize("other", [{"database": "other_db"}, {"table": "other_table"}])
def test_ambiguity_is_scoped_to_database_and_table(
    operations: dict, other: dict
) -> None:
    operations["list_detached_parts"].return_value = [
        make_part(),
        make_part(**other, disks_count=2),
    ]
    assert invoke(operations).exit_code == 0
    operations["drop_detached_part"].assert_called_once()
    operations["logging.warning"].assert_called_once()


def test_unsupported_suffixed_drop_does_not_fall_back_to_disk_removal(
    operations: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    part = make_part("broken_1_1_1_0_try1")
    operations["list_detached_parts"].return_value = [part, make_part("2_2_2_0")]
    operations["drop_detached_part"].side_effect = [
        RuntimeError("BAD_DATA_PART_NAME"),
        None,
    ]
    disk_remove = MagicMock(side_effect=AssertionError("Unexpected disk removal"))
    monkeypatch.setattr(
        "ch_tools.chadmin.internal.clickhouse_disks.ClickHouseDiskClient.remove",
        disk_remove,
    )
    result = invoke(operations)
    assert result.exit_code == 1
    assert operations["drop_detached_part"].call_count == 2
    assert operations["drop_detached_part"].call_args.args[3] == "2_2_2_0"
    disk_remove.assert_not_called()
    operations["logging.error"].assert_called_once()
    error = operations["logging.error"].call_args.args[2]
    assert "BAD_DATA_PART_NAME" in str(error)


def test_failed_inventory_prevents_all_deletions(operations: dict) -> None:
    operations["list_detached_parts"].return_value = [make_part()]
    operations["list_detached_parts"].side_effect = RuntimeError("inventory failed")
    assert invoke(operations).exit_code != 0
    operations["drop_detached_part"].assert_not_called()


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
