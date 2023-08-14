from click import command, option, pass_context

from ch_tools.chadmin.internal.utils import execute_query


@command("settings")
@option("--name")
@option("--changed", is_flag=True)
@pass_context
def list_settings_command(ctx, name, changed):
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
    print(execute_query(ctx, query, name=name, changed=changed))
