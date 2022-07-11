from click import command, pass_context

from cloud.mdb.clickhouse.tools.chadmin.internal.utils import execute_query


@command('async-metrics')
@pass_context
def list_async_metrics_command(ctx):
    print(execute_query(ctx, "SELECT * FROM system.asynchronous_metrics"))
