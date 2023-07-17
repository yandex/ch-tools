from click import command, option, pass_context

from ch_tools.chadmin.internal.utils import execute_query


@command("functions")
@option("--name")
@pass_context
def list_functions_command(ctx, name):
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
    print(execute_query(ctx, query, name=name))
