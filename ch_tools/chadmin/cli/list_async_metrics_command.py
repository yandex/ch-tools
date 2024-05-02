from click import command, pass_context

from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging


@command("async-metrics")
@pass_context
def list_async_metrics_command(ctx):
    """
    Show metrics from system.async_metrics.
    """
    logging.info(execute_query(ctx, "SELECT * FROM system.asynchronous_metrics"))
