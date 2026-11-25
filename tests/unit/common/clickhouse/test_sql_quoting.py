import shutil
import subprocess
from pathlib import Path
from typing import Callable, Optional
from unittest.mock import Mock, patch

import pytest
import requests

from ch_tools.chadmin.internal.partition import detach_partition
from ch_tools.chadmin.internal.table_replica import (
    restart_table_replica,
    restore_table_replica,
)
from ch_tools.chadmin.internal.utils import get_remote_table_for_hosts
from ch_tools.common.clickhouse.client.clickhouse_client import ClickhouseClient
from ch_tools.common.clickhouse.client.error import ClickhouseError
from ch_tools.common.clickhouse.client.sql import (
    qualified_name,
    quote_identifier,
    quote_string,
)
from ch_tools.common.clickhouse.config.clickhouse import ClickhousePort


@pytest.mark.parametrize(
    "header,detail",
    [
        ("60", "HTTP 404 (ClickHouse code 60)"),
        (None, "HTTP 404"),
        ("invalid", "HTTP 404"),
    ],
)
def test_masked_http_error(header: Optional[str], detail: str) -> None:
    response = requests.Response()
    response.status_code = 404
    response._content = b"private"
    if header is not None:
        response.headers["X-ClickHouse-Exception-Code"] = header
    error = ClickhouseError("SELECT ***", response, hide_response=True)
    assert str(error) == f"{detail}\n\nQuery: SELECT ***"


def test_partition_arguments_keep_their_sql_roles() -> None:
    ctx = Mock(obj={"config": {"clickhouse": {"alter_table_timeout": 30}}})
    with patch("ch_tools.chadmin.internal.partition.execute_query") as execute:
        detach_partition(ctx, "db.with.dot", r"a\`b", r"it's\7")
    assert execute.call_args.args[1] == (
        r"ALTER TABLE `db.with.dot`.`a\\``b` DETACH PARTITION ID 'it\'s\\7'"
    )


def test_metadata_patterns_quote_each_value() -> None:
    client = Mock(spec=ClickhouseClient)
    result = ClickhouseClient.render_query(
        client, "name {{ format_str_match(names) }}", names=r"a'b, c\d"
    )
    assert result == r"name IN ('a\'b','c\\d')"


@pytest.mark.skipif(
    shutil.which("clickhouse-local") is None, reason="requires clickhouse-local"
)
def test_catalog_name_round_trip(tmp_path: Path) -> None:
    database = "db.with.dot"
    name = r"a space,percent%\`'{}"
    table = qualified_name(database, name)
    ctx = Mock(obj={"config": {"clickhouse": {"alter_table_timeout": 30}}})
    with patch("ch_tools.chadmin.internal.partition.execute_query") as execute:
        detach_partition(ctx, database, name, "7")
    statements = [
        f"CREATE DATABASE {quote_identifier(database)} ENGINE=Memory",
        f"CREATE TABLE {table} (x UInt8) ENGINE=MergeTree PARTITION BY x ORDER BY x",
        f"INSERT INTO {table} VALUES (7),(8)",
        execute.call_args.args[1],
        f"SELECT x FROM {table}",
        f"SELECT count() FROM system.tables WHERE database={quote_string(database)} AND name={quote_string(name)}",
    ]
    result = subprocess.run(
        [
            "clickhouse-local",
            "--path",
            str(tmp_path),
            "--multiquery",
            "--max_threads=2",
            "--background_schedule_pool_size=4",
        ],
        input=";\n".join(statements),
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.splitlines() == ["8", "1"]


@pytest.mark.parametrize("operation", [restart_table_replica, restore_table_replica])
def test_replica_cluster_literal(operation: Callable) -> None:
    ctx = Mock(
        obj={
            "config": {
                "clickhouse": {
                    "restart_replica_timeout": 1,
                    "restore_replica_timeout": 1,
                }
            }
        }
    )
    with patch("ch_tools.chadmin.internal.table_replica.execute_query") as execute:
        operation(ctx, "db", "table", cluster=r"team's\cluster")
    assert execute.call_args.args[1].endswith(r" ON CLUSTER 'team\'s\\cluster'")


def test_remote_password_uses_request_body_and_stdin() -> None:
    password = r"private'\{{ name }}"
    client = ClickhouseClient(
        host="localhost",
        user="operator",
        password=password,
        timeout=1,
        ports={ClickhousePort.HTTP: 8123, ClickhousePort.TCP: 9000},
    )
    with patch(
        "ch_tools.chadmin.internal.utils.clickhouse_client", return_value=client
    ):
        remote = get_remote_table_for_hosts(Mock(), "system.parts", ["replica"])
    sql = "SELECT * FROM {{ remote }} WHERE name = {{ name|literal }}"
    args = {"remote": remote, "name": "{password}"}
    expected = f"SELECT * FROM remote('replica', system.parts, 'operator', {quote_string(password)}) WHERE name = '{{password}}' FORMAT TSV"
    with patch("requests.post") as post:
        client.query(
            sql,
            port=ClickhousePort.HTTP,
            query_args=args,
            format_="TSV",
            log_query=False,
        )
        request = post.call_args
        assert request.kwargs["data"] == expected.encode()
        assert request.kwargs["headers"]["X-ClickHouse-Key"] == password
        assert request.kwargs["params"] == {}
        assert request.args == ("http://localhost:8123",)

        response = requests.Response()
        response.status_code = 500
        response._content = expected.encode()
        post.return_value = response
        with pytest.raises(ClickhouseError) as http_error:
            client.query(
                sql, port=ClickhousePort.HTTP, query_args=args, log_query=False
            )
        assert "private" not in str(http_error.value)
        assert "500" in str(http_error.value)

    with (
        patch(
            "ch_tools.common.clickhouse.client.client_config._config_path",
            return_value=None,
        ),
        patch("subprocess.Popen") as popen,
    ):
        process = popen.return_value.__enter__.return_value
        process.returncode = 0
        process.communicate.return_value = (b"", b"")
        client.query(
            sql,
            port=ClickhousePort.TCP,
            query_args=args,
            format_="TSV",
            log_query=False,
        )
        assert process.communicate.call_args.kwargs["input"] == expected.encode()
        assert password not in popen.call_args.kwargs["env"].values()
        assert popen.call_args.kwargs["pass_fds"]
        assert all("private" not in arg for arg in popen.call_args.args[0])

        process.returncode = 1
        process.communicate.return_value = (b"", expected.encode())
        with pytest.raises(RuntimeError) as tcp_error:
            client.query(sql, port=ClickhousePort.TCP, query_args=args, log_query=False)
        assert "private" not in str(tcp_error.value)
    assert "private" not in repr(remote)
