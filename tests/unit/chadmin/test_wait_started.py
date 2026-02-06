from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from ch_tools.chadmin.cli.wait_group import (
    BASE_TIMEOUT,
    DEFAULT_MIN_TIMEOUT,
    LOCAL_PART_LOAD_SPEED,
    PID_MAX_CHECK_ATTEMPTS,
    S3_PART_LOAD_SPEED,
    exit_if_pid_not_running,
    get_s3_data_part_count,
    get_timeout_by_files,
    get_timeout_by_parts,
    is_clickhouse_alive,
    is_initial_dictionaries_load_completed,
    wait_group,
)


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def cli_context() -> dict:
    return {
        "config": {
            "loguru": {"handlers": {}},
            "chadmin": {
                "wait": {
                    "ping_command": "timeout 5 sudo -u monitor /usr/bin/ch-monitoring ping"
                }
            },
        }
    }


@pytest.mark.parametrize(
    "local_parts,s3_parts,expected",
    [
        (0, 0, BASE_TIMEOUT),
        (1000, 0, BASE_TIMEOUT + int(1000 / LOCAL_PART_LOAD_SPEED)),
        (0, 500, BASE_TIMEOUT + int(500 / S3_PART_LOAD_SPEED)),
        (
            1000,
            500,
            BASE_TIMEOUT
            + int(1000 / LOCAL_PART_LOAD_SPEED)
            + int(500 / S3_PART_LOAD_SPEED),
        ),
    ],
)
@patch("ch_tools.chadmin.cli.wait_group.get_s3_data_part_count")
@patch("ch_tools.chadmin.cli.wait_group.get_local_data_part_count")
def test_timeout_by_parts_calculation(
    mock_local: MagicMock,
    mock_s3: MagicMock,
    local_parts: int,
    s3_parts: int,
    expected: int,
) -> None:
    mock_local.return_value = local_parts
    mock_s3.return_value = s3_parts
    assert get_timeout_by_parts() == expected


@pytest.mark.parametrize(
    "file_count,speed,min_t,max_t,expected",
    [
        (1000000, None, None, None, 10000),
        (500000, None, None, None, DEFAULT_MIN_TIMEOUT),
        (50000, 100, 300, 1000, 500),
        (1000, 100, 300, 1000, 300),
        (200000, 100, 300, 1000, 1000),
        (0, 100, 300, 1000, 300),
    ],
)
@patch("ch_tools.chadmin.cli.wait_group.get_file_count")
def test_timeout_by_files_calculation(
    mock_file_count: MagicMock,
    file_count: int,
    speed: Optional[int],
    min_t: Optional[int],
    max_t: Optional[int],
    expected: int,
) -> None:
    mock_file_count.return_value = file_count
    assert get_timeout_by_files(speed, min_t, max_t) == expected


@pytest.mark.parametrize(
    "path_exists,cmd_output,expected",
    [
        (True, "3000", 3000),
        (True, "0", 0),
        (False, None, 0),
    ],
)
@patch("ch_tools.chadmin.cli.wait_group.execute")
@patch("os.path.exists")
def test_s3_part_count(
    mock_exists: MagicMock,
    mock_execute: MagicMock,
    path_exists: bool,
    cmd_output: Optional[str],
    expected: int,
) -> None:
    mock_exists.return_value = path_exists
    if cmd_output is not None:
        mock_execute.return_value = cmd_output

    assert get_s3_data_part_count() == expected

    if not path_exists:
        mock_execute.assert_not_called()


@pytest.mark.parametrize(
    "ping_output,raises,expected",
    [
        ("0;OK\n", False, True),
        ("1;FAILED\n", False, False),
        ("", False, False),
        (None, True, False),
    ],
)
@patch("ch_tools.chadmin.cli.wait_group.logging")
@patch("ch_tools.chadmin.cli.wait_group.execute")
@patch("os.chdir")
def test_clickhouse_alive_check(
    _mock_chdir: MagicMock,
    mock_execute: MagicMock,
    _mock_logging: MagicMock,
    ping_output: Optional[str],
    raises: bool,
    expected: bool,
) -> None:
    mock_ctx = MagicMock()
    mock_ctx.obj = {
        "config": {
            "chadmin": {
                "wait": {
                    "ping_command": "timeout 5 sudo -u monitor /usr/bin/ch-monitoring ping"
                }
            }
        }
    }
    if raises:
        mock_execute.side_effect = Exception("Connection failed")
    else:
        mock_execute.return_value = ping_output
    assert is_clickhouse_alive(mock_ctx) == expected


@pytest.mark.parametrize(
    "query_result,wait_failed,raises,expected",
    [
        ("0", False, False, True),
        ("5", False, False, False),
        ("0", True, False, True),
        ("3", True, False, False),
        (None, False, True, False),
    ],
)
@patch("ch_tools.chadmin.cli.wait_group.logging")
@patch("ch_tools.chadmin.cli.wait_group.execute_query")
def test_dictionaries_load_check(
    mock_query: MagicMock,
    _mock_logging: MagicMock,
    query_result: Optional[str],
    wait_failed: bool,
    raises: bool,
    expected: bool,
) -> None:
    if raises:
        mock_query.side_effect = Exception("")
    else:
        mock_query.return_value = query_result
    assert is_initial_dictionaries_load_completed(MagicMock(), wait_failed) == expected


@pytest.mark.parametrize(
    "pid_valid,attempts,should_raise",
    [
        (True, PID_MAX_CHECK_ATTEMPTS + 100, False),
        (False, PID_MAX_CHECK_ATTEMPTS - 1, False),
        (False, PID_MAX_CHECK_ATTEMPTS, True),
    ],
)
@patch("ch_tools.chadmin.cli.wait_group.is_pid_file_valid")
def test_pid_check_raises_after_max_attempts(
    mock_pid_valid: MagicMock,
    pid_valid: bool,
    attempts: int,
    should_raise: bool,
) -> None:
    mock_pid_valid.return_value = pid_valid
    if should_raise:
        with pytest.raises(RuntimeError, match="pid file creation out of max tries"):
            exit_if_pid_not_running("/var/run/clickhouse.pid", attempts)
    else:
        exit_if_pid_not_running("/var/run/clickhouse.pid", attempts)


def test_cli_rejects_invalid_strategy(cli_runner: CliRunner, cli_context: dict) -> None:
    result = cli_runner.invoke(
        wait_group,
        ["started", "--timeout-strategy", "random"],
        obj=cli_context,
    )
    assert result.exit_code != 0


@pytest.mark.parametrize(
    "option",
    [
        ["--file-processing-speed", "100"],
        ["--min-timeout", "300"],
        ["--max-timeout", "1000"],
    ],
)
def test_cli_parts_strategy_rejects_file_options(
    cli_runner: CliRunner, cli_context: dict, option: list
) -> None:
    result = cli_runner.invoke(
        wait_group,
        ["started", "--timeout-strategy", "parts"] + option,
        obj=cli_context,
    )
    assert result.exit_code != 0


def test_cli_files_strategy_rejects_wait_option(
    cli_runner: CliRunner, cli_context: dict
) -> None:
    result = cli_runner.invoke(
        wait_group,
        ["started", "--timeout-strategy", "files", "--wait", "100"],
        obj=cli_context,
    )
    assert result.exit_code != 0


@patch("ch_tools.chadmin.cli.chadmin_group.logging")
@patch("ch_tools.chadmin.cli.wait_group.is_clickhouse_alive")
@patch("ch_tools.chadmin.cli.wait_group.warmup_system_users")
@patch("ch_tools.chadmin.cli.wait_group.is_initial_dictionaries_load_completed")
@patch("ch_tools.chadmin.cli.wait_group.get_timeout_by_parts")
@patch("ch_tools.chadmin.cli.wait_group.time")
def test_cli_parts_strategy_uses_parts_timeout(
    mock_time: MagicMock,
    mock_timeout: MagicMock,
    mock_dict: MagicMock,
    _mock_warmup: MagicMock,
    mock_alive: MagicMock,
    _mock_logging: MagicMock,
    cli_runner: CliRunner,
    cli_context: dict,
) -> None:
    mock_timeout.return_value = 100
    mock_time.time.side_effect = [0, 1, 2]
    mock_time.sleep = MagicMock()
    mock_alive.return_value = True
    mock_dict.return_value = True

    result = cli_runner.invoke(
        wait_group,
        ["started", "--timeout-strategy", "parts"],
        obj=cli_context,
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    mock_timeout.assert_called_once()


@patch("ch_tools.chadmin.cli.chadmin_group.logging")
@patch("ch_tools.chadmin.cli.wait_group.is_clickhouse_alive")
@patch("ch_tools.chadmin.cli.wait_group.warmup_system_users")
@patch("ch_tools.chadmin.cli.wait_group.is_initial_dictionaries_load_completed")
@patch("ch_tools.chadmin.cli.wait_group.get_timeout_by_files")
@patch("ch_tools.chadmin.cli.wait_group.time")
def test_cli_files_strategy_uses_files_timeout(
    mock_time: MagicMock,
    mock_timeout: MagicMock,
    mock_dict: MagicMock,
    _mock_warmup: MagicMock,
    mock_alive: MagicMock,
    _mock_logging: MagicMock,
    cli_runner: CliRunner,
    cli_context: dict,
) -> None:
    mock_timeout.return_value = 100
    mock_time.time.side_effect = [0, 1, 2]
    mock_time.sleep = MagicMock()
    mock_alive.return_value = True
    mock_dict.return_value = True

    result = cli_runner.invoke(
        wait_group,
        ["started", "--timeout-strategy", "files"],
        obj=cli_context,
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    mock_timeout.assert_called_once()


@patch("ch_tools.chadmin.cli.chadmin_group.logging")
@patch("ch_tools.chadmin.cli.wait_group.is_clickhouse_alive")
@patch("ch_tools.chadmin.cli.wait_group.warmup_system_users")
@patch("ch_tools.chadmin.cli.wait_group.is_initial_dictionaries_load_completed")
@patch("ch_tools.chadmin.cli.wait_group.get_timeout_by_files")
@patch("ch_tools.chadmin.cli.wait_group.get_timeout_by_parts")
@patch("ch_tools.chadmin.cli.wait_group.time")
def test_cli_explicit_wait_skips_calculation(
    mock_time: MagicMock,
    mock_parts_timeout: MagicMock,
    mock_files_timeout: MagicMock,
    mock_dict: MagicMock,
    _mock_warmup: MagicMock,
    mock_alive: MagicMock,
    _mock_logging: MagicMock,
    cli_runner: CliRunner,
    cli_context: dict,
) -> None:
    mock_time.time.side_effect = [0, 1, 2]
    mock_time.sleep = MagicMock()
    mock_alive.return_value = True
    mock_dict.return_value = True

    result = cli_runner.invoke(
        wait_group,
        ["started", "--timeout-strategy", "parts", "--wait", "300"],
        obj=cli_context,
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    mock_parts_timeout.assert_not_called()
    mock_files_timeout.assert_not_called()


@patch("ch_tools.chadmin.cli.chadmin_group.logging")
@patch("ch_tools.chadmin.cli.wait_group.is_clickhouse_alive")
@patch("ch_tools.chadmin.cli.wait_group.warmup_system_users")
@patch("ch_tools.chadmin.cli.wait_group.is_initial_dictionaries_load_completed")
@patch("ch_tools.chadmin.cli.wait_group.get_timeout_by_files")
@patch("ch_tools.chadmin.cli.wait_group.time")
def test_cli_passes_custom_file_params(
    mock_time: MagicMock,
    mock_timeout: MagicMock,
    mock_dict: MagicMock,
    _mock_warmup: MagicMock,
    mock_alive: MagicMock,
    _mock_logging: MagicMock,
    cli_runner: CliRunner,
    cli_context: dict,
) -> None:
    mock_timeout.return_value = 100
    mock_time.time.side_effect = [0, 1, 2]
    mock_time.sleep = MagicMock()
    mock_alive.return_value = True
    mock_dict.return_value = True

    result = cli_runner.invoke(
        wait_group,
        [
            "started",
            "--timeout-strategy",
            "files",
            "--file-processing-speed",
            "50",
            "--min-timeout",
            "300",
            "--max-timeout",
            "1000",
        ],
        obj=cli_context,
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    mock_timeout.assert_called_once_with(50, 300, 1000)


@patch("ch_tools.chadmin.cli.chadmin_group.logging")
@patch("ch_tools.chadmin.cli.wait_group.is_clickhouse_alive")
@patch("ch_tools.chadmin.cli.wait_group.get_timeout_by_files")
@patch("ch_tools.chadmin.cli.wait_group.time")
def test_cli_fails_when_clickhouse_dead(
    mock_time: MagicMock,
    mock_timeout: MagicMock,
    mock_alive: MagicMock,
    _mock_logging: MagicMock,
    cli_runner: CliRunner,
    cli_context: dict,
) -> None:
    mock_timeout.return_value = 5
    mock_time.time.side_effect = [0] + [100] * 10
    mock_time.sleep = MagicMock()
    mock_alive.return_value = False

    result = cli_runner.invoke(
        wait_group,
        ["started", "--timeout-strategy", "files"],
        obj=cli_context,
    )

    assert result.exit_code != 0
