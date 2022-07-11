from click import command, pass_context

from cloud.mdb.clickhouse.tools.chadmin.internal.utils import execute_query


@command('events')
@pass_context
def list_events_command(ctx):
    print(execute_query(ctx, "SELECT * FROM system.events"))
