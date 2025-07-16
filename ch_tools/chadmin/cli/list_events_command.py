from click import Context, command, pass_context

from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging


@command("events")
@pass_context
def list_events_command(ctx: Context) -> None:
    """
    Show metrics from system.events.
    """
    logging.info(execute_query(ctx, "SELECT * FROM system.events"))
