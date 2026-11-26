from unittest.mock import MagicMock

import pytest
import requests

from ch_tools.common.clickhouse.client.clickhouse_client import ClickhouseClient


def make_client(max_attempts: int) -> ClickhouseClient:
    return ClickhouseClient(
        host="localhost",
        ports={},
        timeout=1,
        retry_max_attempts=max_attempts,
        retry_max_interval=0,
    )


@pytest.mark.parametrize(
    "configured,override,expected",
    [(3, None, 3), (3, 1, 1), (1, 2, 2)],
)
def test_query_retry_attempts(
    monkeypatch: pytest.MonkeyPatch,
    configured: int,
    override: int | None,
    expected: int,
) -> None:
    client = make_client(configured)
    execute = MagicMock(side_effect=requests.exceptions.ReadTimeout("response lost"))
    monkeypatch.setattr(client, "_execute_query_impl", execute)
    monkeypatch.setattr(
        "ch_tools.common.clickhouse.client.clickhouse_client._log_retry_attempt",
        MagicMock(),
    )

    with pytest.raises(requests.exceptions.ReadTimeout):
        client.query("SELECT 1", retry_max_attempts=override)

    assert execute.call_count == expected


def test_query_override_does_not_change_configured_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = make_client(3)
    execute = MagicMock(side_effect=requests.exceptions.ReadTimeout("response lost"))
    monkeypatch.setattr(client, "_execute_query_impl", execute)
    monkeypatch.setattr(
        "ch_tools.common.clickhouse.client.clickhouse_client._log_retry_attempt",
        MagicMock(),
    )
    with pytest.raises(requests.exceptions.ReadTimeout):
        client.query("DROP DETACHED PART", retry_max_attempts=1)
    assert execute.call_count == 1

    execute.reset_mock()
    execute.side_effect = [requests.exceptions.ReadTimeout("response lost"), "ok"]
    assert client.query("SELECT 1") == "ok"
    assert execute.call_count == 2


@pytest.mark.parametrize("max_attempts", [0, -1])
def test_invalid_retry_override_does_not_send_query(
    monkeypatch: pytest.MonkeyPatch, max_attempts: int
) -> None:
    client = make_client(3)
    execute = MagicMock()
    monkeypatch.setattr(client, "_execute_query_impl", execute)
    with pytest.raises(ValueError, match="at least 1"):
        client.query("SELECT 1", retry_max_attempts=max_attempts)
    execute.assert_not_called()
