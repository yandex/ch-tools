from click import command, pass_context

from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging


@command("macros")
@pass_context
def list_macros_command(ctx):
    """
    Show macros.
    """
    logging.info(execute_query(ctx, "SELECT * FROM system.macros"))
