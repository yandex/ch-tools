import shutil
import subprocess
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from ch_tools.chadmin.internal.partition import detach_partition
from ch_tools.common.clickhouse.client.clickhouse_client import ClickhouseClient
from ch_tools.common.clickhouse.client.query import Query
from ch_tools.common.clickhouse.client.sql import (
    qualified_name,
    quote_identifier,
    quote_string,
)
from ch_tools.common.clickhouse.config.clickhouse import ClickhousePort


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


def test_query_parameters_survive_template_rendering_on_both_transports() -> None:
    client = ClickhouseClient(
        host="localhost",
        timeout=1,
        ports={ClickhousePort.HTTP: 8123, ClickhousePort.TCP: 9000},
    )
    value = "ordinary\\value\twith\na quote'"
    query = Query(
        "SELECT {sample:String}, {{ label|literal }}", parameters={"sample": value}
    )
    label = "{sample:String}"
    with patch("requests.post") as post:
        client.query(
            query,
            port=ClickhousePort.HTTP,
            query_args={"label": label},
            format_="TSV",
            log_query=False,
        )
    params = post.call_args.kwargs["params"]
    assert params["param_sample"] == r"ordinary\\value\twith\na quote'"
    assert params["query"] == "SELECT {sample:String}, '{sample:String}' FORMAT TSV"
    with patch("subprocess.Popen") as popen:
        process = popen.return_value
        process.returncode = 0
        process.communicate.return_value = (b"", b"")
        client.query(
            query,
            port=ClickhousePort.TCP,
            query_args={"label": label},
            format_="TSV",
            log_query=False,
        )
    assert "--param_sample=" + params["param_sample"] in popen.call_args.args[0]
    assert process.communicate.call_args.kwargs["input"] == params["query"].encode()
    assert value not in repr(query)
