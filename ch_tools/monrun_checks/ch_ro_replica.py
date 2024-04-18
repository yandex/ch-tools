import click

from ch_tools.common.clickhouse.client.clickhouse_client import clickhouse_client
from ch_tools.common.result import CRIT, OK, Result


@click.command("ro-replica")
@click.pass_context
def ro_replica_command(ctx):
    """
    Check for readonly replicated tables.
    """
    query = "SELECT database, table FROM system.replicas WHERE is_readonly"
    response = clickhouse_client(ctx).query_json_data(query, compact=False)
    if response:
        tables_str = ", ".join(
            f"{item['database']}.{item['table']}" for item in response
        )
        return Result(CRIT, f"Readonly replica tables: {tables_str}")

    return Result(OK)
