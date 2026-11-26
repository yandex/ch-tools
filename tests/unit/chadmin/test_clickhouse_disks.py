import subprocess
from unittest.mock import MagicMock, patch

import pytest

from ch_tools.chadmin.internal.clickhouse_disks import (
    CLICKHOUSE_DISKS_TIMEOUT,
    ClickHouseDiskClient,
)


@patch("ch_tools.chadmin.internal.clickhouse_disks.logging")
@patch("ch_tools.chadmin.internal.clickhouse_disks.subprocess.run")
def test_clickhouse_disks_uses_argument_list_without_shell(
    run: MagicMock,
    _logging: MagicMock,
) -> None:
    run.return_value = MagicMock(returncode=0, stdout=b"data", stderr=b"")
    client = ClickHouseDiskClient("s3", "/tmp/disks.xml")

    assert client.read("path with spaces/file.bin") == b"data"

    arguments = run.call_args.args[0]
    assert arguments[:6] == [
        "sudo",
        "-u",
        "clickhouse",
        "env",
        "HOME=/tmp",
        "clickhouse-disks",
    ]
    assert arguments[-2:] == ["--query", "read 'path with spaces/file.bin'"]
    assert run.call_args.kwargs["check"] is False
    assert "shell" not in run.call_args.kwargs
    assert run.call_args.kwargs["timeout"] == CLICKHOUSE_DISKS_TIMEOUT


@patch("ch_tools.chadmin.internal.clickhouse_disks.logging")
@patch("ch_tools.chadmin.internal.clickhouse_disks.subprocess.run")
def test_clickhouse_disks_reports_timeout(
    run: MagicMock,
    _logging: MagicMock,
) -> None:
    run.side_effect = subprocess.TimeoutExpired(
        ["clickhouse-disks"], CLICKHOUSE_DISKS_TIMEOUT
    )
    client = ClickHouseDiskClient("s3", "/tmp/disks.xml")

    with pytest.raises(
        RuntimeError,
        match=f"timed out after {CLICKHOUSE_DISKS_TIMEOUT} seconds",
    ):
        client.read("file.bin")


@pytest.mark.parametrize(
    "parents,expected_query",
    [
        (True, "mkdir 'nested/path' --parents"),
        (False, "mkdir 'nested/path'"),
    ],
)
@patch("ch_tools.chadmin.internal.clickhouse_disks.logging")
@patch("ch_tools.chadmin.internal.clickhouse_disks.subprocess.run")
def test_clickhouse_disks_mkdir_uses_query_interface(
    run: MagicMock,
    _logging: MagicMock,
    parents: bool,
    expected_query: str,
) -> None:
    run.return_value = MagicMock(returncode=0, stdout=b"", stderr=b"")
    client = ClickHouseDiskClient("s3", "/tmp/disks.xml")

    client.mkdir("nested/path", parents=parents)

    arguments = run.call_args.args[0]
    assert arguments[-2:] == ["--query", expected_query]


@patch("ch_tools.chadmin.internal.clickhouse_disks.logging")
@patch("ch_tools.chadmin.internal.clickhouse_disks.subprocess.run")
def test_clickhouse_disks_copy_and_recursive_remove_use_query_interface(
    run: MagicMock,
    _logging: MagicMock,
) -> None:
    run.return_value = MagicMock(returncode=0, stdout=b"", stderr=b"")
    client = ClickHouseDiskClient("s3", "/tmp/disks.xml")

    client.copy("source/file.bin", "target/file.bin")
    client.remove("target", recursive=True)

    assert run.call_args_list[0].args[0][-2:] == [
        "--query",
        "copy 'source/file.bin' 'target/file.bin'",
    ]
    assert run.call_args_list[1].args[0][-2:] == [
        "--query",
        "remove 'target' --recursive",
    ]


@patch("ch_tools.chadmin.internal.clickhouse_disks.logging")
@patch("ch_tools.chadmin.internal.clickhouse_disks.subprocess.run")
def test_clickhouse_disks_remove_returns_failure_when_check_is_disabled(
    run: MagicMock,
    _logging: MagicMock,
) -> None:
    run.return_value = MagicMock(returncode=7, stdout=b"", stderr=b"failed")
    client = ClickHouseDiskClient("s3", run_as_clickhouse=False)

    result = client.remove(
        "test/path",
        recursive=True,
        check=False,
    )

    assert result == (7, b"failed")
    assert run.call_args.args[0] == [
        "clickhouse-disks",
        "--disk",
        "s3",
        "--query",
        "remove 'test/path' --recursive",
    ]


@patch("ch_tools.chadmin.internal.clickhouse_disks.logging")
@patch("ch_tools.chadmin.internal.clickhouse_disks.subprocess.run")
def test_clickhouse_disks_query_error_is_not_hidden_by_zero_exit_code(
    run: MagicMock,
    _logging: MagicMock,
) -> None:
    run.return_value = subprocess.CompletedProcess(
        [], 0, b"", b"Error: Code: 76. Cannot open file"
    )
    client = ClickHouseDiskClient("s3", "/tmp/disks.xml")

    with pytest.raises(RuntimeError, match="Cannot open file"):
        client.mkdir("recovery/stage", parents=True)
