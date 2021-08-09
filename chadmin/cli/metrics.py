from click import command, pass_context

from . import execute_query


@command('metrics')
@pass_context
def list_metrics_command(ctx):
    print(execute_query(ctx, "SELECT * FROM system.metrics"))
