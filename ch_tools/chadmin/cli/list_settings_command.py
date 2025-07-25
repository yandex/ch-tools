from click import Context, command, option, pass_context

from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging


@command("settings")
@option("--name")
@option("--changed", is_flag=True)
@pass_context
def list_settings_command(ctx: Context, name: str, changed: bool) -> None:
    """
    Show settings.
    """
    query = """
        SELECT *
        FROM system.settings
        WHERE 1
        {% if name %}
          AND lower(name) {{ format_str_imatch(name) }}
        {% endif %}
        {% if changed %}
          AND changed
        {% endif %}
        """
    logging.info(execute_query(ctx, query, name=name, changed=changed))
