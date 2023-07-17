from click import argument, group, pass_context

from ch_tools.chadmin.internal.table_replica import (
    get_table_replica,
    restart_table_replica,
)
from ch_tools.common.cli.formatting import print_response


@group("table-replica")
def table_replica_group():
    """Commands to manage table replicas."""
    pass


@table_replica_group.command("get")
@argument("database")
@argument("table")
@pass_context
def get_replica_command(ctx, database, table):
    print_response(ctx, get_table_replica(ctx, database, table))


@table_replica_group.command("restart")
@argument("database")
@argument("table")
@pass_context
def restart_replica_command(ctx, database, table):
    """
    Perform SYSTEM RESTART REPLICA for the specified replicated table.
    """
    restart_table_replica(ctx, database, table)
