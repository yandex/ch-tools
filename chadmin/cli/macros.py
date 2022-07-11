from click import command, pass_context

from cloud.mdb.clickhouse.tools.chadmin.internal.utils import execute_query


@command('macros')
@pass_context
def list_macros_command(ctx):
    print(execute_query(ctx, "SELECT * FROM system.macros"))
