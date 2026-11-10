from typing import Any, Iterator
from unittest.mock import MagicMock, patch

import pytest
from click import Command, Context

from ch_tools.chadmin.internal.object_storage import replicas
from ch_tools.chadmin.internal.table import delete_table
from ch_tools.chadmin.internal.utils import (
    check_replicas_availability,
    execute_query_on_shard,
    get_unavailable_replicas,
)


@pytest.fixture(autouse=True)
def mock_logging() -> Iterator[None]:
    with (
        patch.object(replicas, "logging"),
        patch("ch_tools.chadmin.internal.utils.logging"),
        patch("ch_tools.chadmin.internal.table.logging"),
    ):
        yield


def make_context(**config: Any) -> Context:
    return Context(
        Command("test"),
        obj={
            "config": {
                "object_storage": config,
                "clickhouse": {"drop_table_timeout": 60},
            }
        },
    )


@pytest.mark.parametrize("value", ["host", None, [""], ["   "], [123]])
def test_invalid_host_list(value: Any) -> None:
    with pytest.raises(ValueError, match="unavailable_hosts"):
        replicas.get_object_storage_replicas(make_context(unavailable_hosts=value))


@patch.object(
    replicas.ClickhouseInfo,
    "get_replicas",
    return_value=["local", "vla-live", "vla-down", "host"],
)
@patch.object(replicas, "get_unavailable_replicas", return_value=["vla-down", "host"])
def test_selection(probe: MagicMock, discovery: MagicMock) -> None:
    ctx = make_context(
        unavailable_hosts=["vla-live", "vla-down", "host"],
    )
    selected = replicas.get_object_storage_replicas(ctx)
    assert selected.hosts == ["local", "vla-live"]
    assert selected.excluded == ["vla-down", "host"]
    probe.assert_called_once_with(ctx, replicas=discovery.return_value)


@pytest.mark.parametrize("host", ["vla-", "vla-*", "VLA-down", "vla-down.example.net"])
@patch.object(
    replicas.ClickhouseInfo, "get_replicas", return_value=["local", "vla-down"]
)
@patch.object(replicas, "get_unavailable_replicas", return_value=["vla-down"])
def test_host_name_must_match_exactly(
    _probe: MagicMock, _discovery: MagicMock, host: str
) -> None:
    with pytest.raises(RuntimeError, match="exemption: vla-down"):
        replicas.get_object_storage_replicas(make_context(unavailable_hosts=[host]))


@patch.object(replicas.ClickhouseInfo, "get_replicas", return_value=["local"])
@patch.object(replicas, "get_unavailable_replicas", return_value=[])
def test_no_exemptions(_probe: MagicMock, _discovery: MagicMock) -> None:
    assert replicas.get_object_storage_replicas(make_context()).hosts == ["local"]
    replicas.logging.warning.assert_not_called()  # type: ignore[attr-defined]


@patch.object(
    replicas.ClickhouseInfo,
    "get_replicas",
    return_value=["local", "allowed", "unexpected"],
)
@patch.object(
    replicas, "get_unavailable_replicas", return_value=["allowed", "unexpected"]
)
def test_unexpected_failure_blocks_selection(
    _probe: MagicMock, _discovery: MagicMock
) -> None:
    ctx = make_context(
        unavailable_hosts=["allowed"],
    )
    with pytest.raises(RuntimeError, match="exemption: unexpected"):
        replicas.get_object_storage_replicas(ctx)


@patch.object(replicas.ClickhouseInfo, "get_replicas")
@patch.object(replicas, "get_unavailable_replicas")
def test_no_participants(probe: MagicMock, discovery: MagicMock) -> None:
    ctx = make_context(
        unavailable_hosts=["host"],
    )
    discovery.return_value = []
    with pytest.raises(RuntimeError, match="No replicas found"):
        replicas.get_object_storage_replicas(ctx)
    probe.assert_not_called()
    discovery.return_value = ["host"]
    probe.return_value = ["host"]
    with pytest.raises(RuntimeError, match="No available replicas"):
        replicas.get_object_storage_replicas(ctx)
    discovery.side_effect = RuntimeError("discovery failed")
    with pytest.raises(RuntimeError, match="discovery failed"):
        replicas.get_object_storage_replicas(ctx)


@patch("ch_tools.chadmin.internal.utils.execute_query")
@patch.object(
    replicas.ClickhouseInfo, "get_replicas", return_value=["down1", "local", "down2"]
)
def test_probe_collects_all_failures(discovery: MagicMock, query: MagicMock) -> None:
    ctx = make_context()
    query.side_effect = [RuntimeError("offline"), "1", RuntimeError("offline")]
    assert get_unavailable_replicas(ctx) == ["down1", "down2"]
    assert [
        call.kwargs["replica"] for call in query.call_args_list
    ] == discovery.return_value
    query.side_effect = None
    assert check_replicas_availability(ctx)
    query.side_effect = RuntimeError("offline")
    assert not check_replicas_availability(ctx)


@patch("ch_tools.chadmin.internal.utils.execute_query")
@patch.object(replicas.ClickhouseInfo, "get_replicas")
def test_explicit_participants(discovery: MagicMock, query: MagicMock) -> None:
    ctx = make_context()
    delete_table(ctx, "db", "table", shard=True, replicas=["selected"])
    assert query.call_args.kwargs["replica"] == "selected"
    query.reset_mock()
    delete_table(ctx, "db", "table", shard=True, replicas=[])
    execute_query_on_shard(ctx, "SELECT 1", replicas=[])
    assert get_unavailable_replicas(ctx, replicas=[]) == []
    query.assert_not_called()
    discovery.assert_not_called()
