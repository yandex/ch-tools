import click

from ch_tools.common.clickhouse.client.clickhouse_client import clickhouse_client
from ch_tools.common.result import Result


@click.command("ro-replica")
@click.pass_context
def ro_replica_command(ctx):
    """
    Check for readonly replicated tables.
    """
    response = clickhouse_client(ctx).query_json_data(
        query="SELECT database, table FROM system.replicas WHERE is_readonly"
    )
    if response:
        return Result(2, f"Readonly replica tables: {response}")

    return Result(0, "OK")
