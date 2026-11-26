import shutil
import subprocess
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests

from ch_tools.chadmin.internal.partition import detach_partition, move_partition
from ch_tools.chadmin.internal.utils import get_remote_table_for_hosts
from ch_tools.common.clickhouse.client.clickhouse_client import ClickhouseClient
from ch_tools.common.clickhouse.client.error import ClickhouseError
from ch_tools.common.clickhouse.client.parameters import (
    Identifier,
    Parameter,
    encode_parameters,
)
from ch_tools.common.clickhouse.client.sql import (
    qualified_name,
    quote_identifier,
    quote_string,
)
from ch_tools.common.clickhouse.client.utils import render_template
from ch_tools.common.clickhouse.config.clickhouse import ClickhousePort


def test_move_partition_quotes_destination_and_binds_supported_operands() -> None:
    client = ClickhouseClient(
        host="localhost", ports={ClickhousePort.HTTP: 8123}, timeout=1
    )
    ctx = Mock(
        obj={
            "chcli": client,
            "config": {"clickhouse": {"version": "24.8", "alter_table_timeout": 30}},
        }
    )
    with patch("requests.post") as post, patch("ch_tools.common.logging.debug"):
        move_partition(ctx, "src", "source", "7", "dst", "table.with.dot")
    fields = post.call_args.kwargs["files"]
    query = fields["query"][1].decode()
    assert query.endswith(" TO TABLE `dst`.`table.with.dot`")
    assert fields["param_ch_tools_0"] == (None, "src")
    assert fields["param_ch_tools_1"] == (None, "source")
    assert "MOVE PARTITION '7'" in query


@pytest.mark.skipif(
    shutil.which("clickhouse-local") is None, reason="requires clickhouse-local"
)
def test_catalog_name_round_trip(tmp_path: Path) -> None:
    database = "db.with.dot"
    name = r"a space,percent%\`'{}"
    table = qualified_name(database, name)
    ctx = Mock(
        obj={"config": {"clickhouse": {"version": "24.8", "alter_table_timeout": 30}}}
    )
    with patch("ch_tools.chadmin.internal.partition.execute_query") as execute:
        detach_partition(ctx, database, name, "7")
    params: dict[str, Parameter] = {}
    client = Mock(spec=ClickhouseClient)
    sql = render_template(
        execute.call_args.args[1],
        execute.call_args.kwargs,
        client.get_clickhouse_version,
        params,
    )
    statements = [
        f"CREATE DATABASE {quote_identifier(database)} ENGINE=Memory",
        f"CREATE TABLE {table} (x UInt8) ENGINE=MergeTree PARTITION BY x ORDER BY x",
        f"INSERT INTO {table} VALUES (7),(8)",
        sql,
        f"SELECT x FROM {table}",
        f"SELECT count() FROM system.tables WHERE database={quote_string(database)} AND name={quote_string(name)}",
    ]
    result = subprocess.run(
        [
            "clickhouse-local",
            "--path",
            str(tmp_path),
            "--multiquery",
            *(f"--param_{k}={v}" for k, v in encode_parameters(params).items()),
            "--max_threads=2",
            "--background_schedule_pool_size=4",
        ],
        input=";\n".join(statements),
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.splitlines() == ["8", "1"]


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
    sql = "SELECT * FROM {{ remote }} WHERE name = {{ name|bind }}"
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
        response.status_code = 404
        response.headers["X-ClickHouse-Exception-Code"] = "60"
        response._content = expected.encode()
        post.return_value = response
        with pytest.raises(ClickhouseError) as http_error:
            client.query(
                sql, port=ClickhousePort.HTTP, query_args=args, log_query=False
            )
        assert "private" not in str(http_error.value)
        assert "HTTP 404 (ClickHouse code 60)" in str(http_error.value)

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


@pytest.mark.parametrize("transport", [ClickhousePort.HTTP, ClickhousePort.TCP])
def test_explicit_bindings_on_both_transports(transport: ClickhousePort) -> None:
    client = ClickhouseClient(host="localhost", ports={transport: 8123}, timeout=1)
    values: dict[str, Parameter] = {
        "db": Identifier(r"db\t"),
        "value": "a\tb",
        "names": ["a'b", r"one\two"],
    }
    sql = "SELECT {value:String} IN {names:Array(String)} FROM {db:Identifier}.tables"
    wire = {"db": r"db\t", "value": r"a\tb", "names": r"['a\'b','one\\two']"}
    with patch("requests.post") as post, patch("subprocess.Popen") as popen:
        proc = popen.return_value.__enter__.return_value
        proc.communicate.return_value = (b"1", b"")
        proc.returncode = 0
        client.query(sql, parameters=values, port=transport, log_query=False)
        if transport == ClickhousePort.HTTP:
            fields = post.call_args.kwargs["files"]
            assert fields == {
                "query": (None, sql.encode()),
                **{f"param_{k}": (None, v) for k, v in wire.items()},
            }
            assert post.call_args.kwargs["params"] == {}
        else:
            assert popen.call_args.args[0][-3:] == [
                f"--param_{k}={v}" for k, v in wire.items()
            ]
            proc.communicate.assert_called_once_with(input=sql.encode())
