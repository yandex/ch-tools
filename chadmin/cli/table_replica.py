from click import argument, group, pass_context
from cloud.mdb.cli.common.formatting import print_response
from cloud.mdb.clickhouse.tools.chadmin.internal.table_replica import get_table_replica
from cloud.mdb.clickhouse.tools.chadmin.internal.utils import execute_query


@group('table-replica')
def table_replica_group():
    """Command to manage table replicas."""
    pass


@table_replica_group.command('get')
@argument('database')
@argument('table')
@pass_context
def get_replica_command(ctx, database, table):
    print_response(ctx, get_table_replica(ctx, database, table))


@table_replica_group.command('restart')
@argument('database')
@argument('table')
@pass_context
def restart_replica_command(ctx, database, table):
    """
    Perform SYSTEM RESTART REPLICA for the specified replicated table.
    """
    query = f"""SYSTEM RESTART REPLICA `{database}`.`{table}`"""
    execute_query(ctx, query, format=None)
