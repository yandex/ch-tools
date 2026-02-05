"""
Unit tests for chadmin server restart command.
"""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from ch_tools.chadmin.cli.server_group import server_group


@pytest.fixture
def cli_runner() -> CliRunner:
    """Create CLI runner."""
    return CliRunner()


@pytest.fixture
def cli_context() -> dict:
    """Create CLI context with test configuration."""
    return {
        "config": {
            "chadmin": {
                "server": {
                    "restart": {
                        "command": "echo 'restart'",
                        "timeout": 60,
                        "check_interval": 1,
                    }
                }
            },
            "loguru": {"handlers": {}},
        }
    }


@patch("ch_tools.chadmin.cli.server_group.execute")
@patch("ch_tools.chadmin.cli.server_group.execute_query")
@patch("ch_tools.chadmin.cli.server_group.warmup_system_users")
@patch("ch_tools.chadmin.cli.server_group.is_initial_dictionaries_load_completed")
@patch("ch_tools.chadmin.cli.server_group.time")
def test_restart_success(
    mock_time: MagicMock,
    mock_dict: MagicMock,
    mock_warmup: MagicMock,
    mock_query: MagicMock,
    mock_execute: MagicMock,
    cli_runner: CliRunner,
    cli_context: dict,
) -> None:
    """Test successful server restart."""
    # Simulate time progression
    mock_time.time.side_effect = [0, 1, 2]
    mock_time.sleep = MagicMock()

    # Server responds with low uptime (indicating restart)
    mock_query.return_value = "1"

    # Dictionaries loaded
    mock_dict.return_value = True

    result = cli_runner.invoke(
        server_group,
        ["restart"],
        obj=cli_context,
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    mock_execute.assert_called_once()
    mock_warmup.assert_called_once()


@patch("ch_tools.chadmin.cli.server_group.execute")
@patch("ch_tools.chadmin.cli.server_group.execute_query")
@patch("ch_tools.chadmin.cli.server_group.time")
def test_restart_timeout_server_not_responding(
    mock_time: MagicMock,
    mock_query: MagicMock,
    mock_execute: MagicMock,
    cli_runner: CliRunner,
    cli_context: dict,
) -> None:
    """Test timeout when server doesn't respond."""
    # Simulate timeout
    mock_time.time.side_effect = [0] + [100] * 10
    mock_time.sleep = MagicMock()

    # Server never responds to queries
    mock_query.side_effect = Exception("Connection failed")

    result = cli_runner.invoke(
        server_group,
        ["restart", "--timeout", "5"],
        obj=cli_context,
    )

    assert result.exit_code != 0
    assert "didn't fully start" in result.output.lower()


@patch("ch_tools.chadmin.cli.server_group.execute")
@patch("ch_tools.chadmin.cli.server_group.execute_query")
@patch("ch_tools.chadmin.cli.server_group.time")
def test_restart_timeout_uptime_check(
    mock_time: MagicMock,
    mock_query: MagicMock,
    mock_execute: MagicMock,
    cli_runner: CliRunner,
    cli_context: dict,
) -> None:
    """Test timeout during uptime check."""
    # Simulate timeout
    mock_time.time.side_effect = [0, 1, 100, 101]
    mock_time.sleep = MagicMock()

    # Server responds but uptime check always shows high uptime (server didn't restart)
    mock_query.return_value = "1000"

    result = cli_runner.invoke(
        server_group,
        ["restart", "--timeout", "5"],
        obj=cli_context,
    )

    assert result.exit_code != 0
    assert "didn't fully start" in result.output.lower()


@patch("ch_tools.chadmin.cli.server_group.execute")
def test_restart_command_failure(
    mock_execute: MagicMock,
    cli_runner: CliRunner,
    cli_context: dict,
) -> None:
    """Test failure when restart command fails."""
    # Simulate command failure
    mock_execute.side_effect = Exception("Command failed")

    result = cli_runner.invoke(
        server_group,
        ["restart"],
        obj=cli_context,
    )

    assert result.exit_code != 0
    assert "failed to execute restart command" in result.output.lower()


@patch("ch_tools.chadmin.cli.server_group.execute")
@patch("ch_tools.chadmin.cli.server_group.execute_query")
@patch("ch_tools.chadmin.cli.server_group.warmup_system_users")
@patch("ch_tools.chadmin.cli.server_group.is_initial_dictionaries_load_completed")
@patch("ch_tools.chadmin.cli.server_group.time")
def test_restart_with_custom_timeout(
    mock_time: MagicMock,
    mock_dict: MagicMock,
    mock_warmup: MagicMock,
    mock_query: MagicMock,
    mock_execute: MagicMock,
    cli_runner: CliRunner,
    cli_context: dict,
) -> None:
    """Test restart with custom timeout parameter."""
    # Simulate time progression with custom timeout of 120s and check_interval of 1s
    mock_time.time.side_effect = [0, 1, 2]
    mock_time.sleep = MagicMock()
    mock_query.return_value = "1"
    mock_dict.return_value = True

    result = cli_runner.invoke(
        server_group,
        ["restart", "--timeout", "120"],
        obj=cli_context,
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    # Verify the restart succeeded with custom timeout
    mock_execute.assert_called_once()
    mock_warmup.assert_called_once()
    mock_dict.assert_called_once()


@patch("ch_tools.chadmin.cli.server_group.execute")
@patch("ch_tools.chadmin.cli.server_group.execute_query")
@patch("ch_tools.chadmin.cli.server_group.warmup_system_users")
@patch("ch_tools.chadmin.cli.server_group.is_initial_dictionaries_load_completed")
@patch("ch_tools.chadmin.cli.server_group.time")
def test_restart_timeout_dictionaries_never_load(
    mock_time: MagicMock,
    mock_dict: MagicMock,
    mock_warmup: MagicMock,
    mock_query: MagicMock,
    mock_execute: MagicMock,
    cli_runner: CliRunner,
    cli_context: dict,
) -> None:
    """Test timeout when dictionaries never finish loading after successful restart."""
    # Simulate time progression: restart succeeds but dictionaries timeout
    # Provide enough time values for multiple loop iterations
    # Start at 0, then progress through iterations until timeout at 61
    time_values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 61, 62]
    mock_time.time.side_effect = time_values
    mock_time.sleep = MagicMock()

    # Server responds with low uptime (indicating restart) on each check
    mock_query.return_value = "1"

    # Dictionaries never report as fully loaded
    mock_dict.return_value = False

    result = cli_runner.invoke(
        server_group,
        ["restart", "--timeout", "60"],
        obj=cli_context,
    )

    # Expect non-zero exit code due to timeout waiting for dictionaries
    assert result.exit_code != 0
    assert "didn't fully start" in result.output.lower()

    # Restart should have been attempted once
    mock_execute.assert_called_once()

    # Warmup and dict check should be called multiple times (once per loop iteration)
    assert mock_warmup.call_count >= 2
    assert mock_dict.call_count >= 2


@patch("ch_tools.chadmin.cli.server_group.execute")
@patch("ch_tools.chadmin.cli.server_group.execute_query")
@patch("ch_tools.chadmin.cli.server_group.warmup_system_users")
@patch("ch_tools.chadmin.cli.server_group.is_initial_dictionaries_load_completed")
@patch("ch_tools.chadmin.cli.server_group.time")
def test_restart_warmup_system_users_failure(
    mock_time: MagicMock,
    mock_dict: MagicMock,
    mock_warmup: MagicMock,
    mock_query: MagicMock,
    mock_execute: MagicMock,
    cli_runner: CliRunner,
    cli_context: dict,
) -> None:
    """Test failure when warmup_system_users raises an exception after successful restart."""
    # Provide enough time values - warmup exception is caught, loop continues until timeout
    time_values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 61, 62]
    mock_time.time.side_effect = time_values
    mock_time.sleep = MagicMock()

    # Server responds with low uptime (indicating restart)
    mock_query.return_value = "1"

    # Warmup fails after restart - will be caught and loop continues
    mock_warmup.side_effect = RuntimeError("warmup failed")

    result = cli_runner.invoke(
        server_group,
        ["restart"],
        obj=cli_context,
    )

    # Restart should be treated as failed because warmup keeps failing until timeout
    assert result.exit_code != 0
    # The timeout message should appear since warmup keeps failing
    assert "didn't fully start" in result.output.lower()

    mock_execute.assert_called_once()
    # Warmup will be called multiple times as the loop retries
    assert mock_warmup.call_count >= 1
    # Dictionaries check should not be called if warmup fails
    mock_dict.assert_not_called()
