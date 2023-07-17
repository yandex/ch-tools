from click import command, pass_context

from ch_tools.chadmin.internal.utils import execute_query


@command("metrics")
@pass_context
def list_metrics_command(ctx):
    """
    Show metrics from system.metrics.
    """
    print(execute_query(ctx, "SELECT * FROM system.metrics"))
