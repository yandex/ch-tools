import pathlib
import time
from typing import Any
from urllib.parse import quote

import click

from ch_tools.common.clickhouse.client.clickhouse_client import clickhouse_client
from ch_tools.common.result import Result


@click.command("dist-tables")
@click.option(
    "-c", "--critical", "crit", type=int, default=3600, help="Critical threshold."
)
@click.option(
    "-w", "--warning", "warn", type=int, default=600, help="Warning threshold."
)
@click.pass_context
def dist_tables_command(ctx: click.Context, crit: int, warn: int) -> Result:
    """
    Check for old chunks on Distributed tables.
    """

    status = 0
    issues = []

    ch_client = clickhouse_client(ctx)

    query = "SELECT database, name FROM system.tables WHERE engine = 'Distributed'"
    distributed_tables = ch_client.query_json_data(query=query, compact=False)
    for table in distributed_tables:
        tss = get_chunk_timestamps(table)
        if tss["broken"]:
            issues.append(
                f'{table["database"]}.{table["name"]}: {len(tss["broken"])} broken chunks'
            )
            status = max(1, status)

        oldest_ts, oldest_fn = tss["root"] and tss["root"][0] or (None, None)
        if not oldest_ts:
            continue
        timespan = int(time.time()) - oldest_ts
        if timespan < warn:
            continue

        if timespan < crit:
            status = max(1, status)
        else:
            status = 2

        issues.append(
            f'{table["database"],}.{table["name"]}: {oldest_fn} ({int(timespan)})'
        )

    message = ", ".join(issues)
    return Result(status, message or "OK")


def get_chunk_timestamps(table: Any) -> Any:
    """
    Return timestamps of files contained within dist table directory.
    """
    path = pathlib.Path(get_table_path(table))

    patterns = {
        "broken": "*/broken/*",
        "root": "*/*",
    }
    return {
        subdir: sorted(
            [(f.stat().st_atime, f.name) for f in path.glob(pattern) if f.is_file()]
        )
        for subdir, pattern in patterns.items()
    }


def get_table_path(table: Any) -> str:
    """
    Return path to table directory on file system.
    """
    db_name = quote(table["database"], safe="")
    table_name = quote(table["name"], safe="")
    return f"/var/lib/clickhouse/data/{db_name}/{table_name}"
