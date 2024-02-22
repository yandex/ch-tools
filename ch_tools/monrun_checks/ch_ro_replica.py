import click

from ch_tools.common.result import Result
from ch_tools.monrun_checks.utils import execute_query


@click.command("ro-replica")
@click.pass_context
def ro_replica_command(ctx):
    """
    Check for readonly replicated tables.
    """
    response = execute_query(
        ctx, query="SELECT database, table FROM system.replicas WHERE is_readonly"
    )
    if response:
        return Result(2, f"Readonly replica tables: {response}")

    return Result(0, "OK")
