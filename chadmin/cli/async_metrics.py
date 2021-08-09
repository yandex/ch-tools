from click import command, pass_context

from . import execute_query


@command('async-metrics')
@pass_context
def list_async_metrics_command(ctx):
    print(execute_query(ctx, "SELECT * FROM system.asynchronous_metrics"))
