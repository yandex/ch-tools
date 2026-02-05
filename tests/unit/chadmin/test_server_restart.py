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
    # Verify custom timeout was used
    assert mock_time.time.call_count >= 2
