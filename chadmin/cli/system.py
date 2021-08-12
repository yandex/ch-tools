from click import argument, group, pass_context
from cloud.mdb.clickhouse.tools.chadmin.cli import execute_query


@group('system')
def system_group():
    pass


@system_group.command('restart-replica')
@argument('database')
@argument('table')
@pass_context
def restart_replica_command(ctx, database, table):
    query = f"""SYSTEM RESTART REPLICA `{database}`.`{table}`"""
    execute_query(ctx, query, format=None)
