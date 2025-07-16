from click import Context, command, option, pass_context

from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging


@command("functions")
@option("--name")
@pass_context
def list_functions_command(ctx: Context, name: str) -> None:
    """
    Show available functions.
    """
    query = """
        SELECT *
        FROM system.functions
        {% if name %}
        WHERE lower(name) {{ format_str_imatch(name) }}
        {% endif %}
        """
    logging.info(execute_query(ctx, query, name=name))
