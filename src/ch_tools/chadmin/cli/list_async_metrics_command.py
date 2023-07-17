from click import command, pass_context

from ch_tools.chadmin.internal.utils import execute_query


@command("async-metrics")
@pass_context
def list_async_metrics_command(ctx):
    """
    Show metrics from system.async_metrics.
    """
    print(execute_query(ctx, "SELECT * FROM system.asynchronous_metrics"))
