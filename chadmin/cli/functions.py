from click import command, option, pass_context

from cloud.mdb.clickhouse.tools.chadmin.internal.utils import execute_query


@command('functions')
@option('--name')
@pass_context
def list_functions_command(ctx, name):
    query = """
        SELECT *
        FROM system.functions
        {% if name %}
        WHERE lower(name) {{ format_str_imatch(name) }}
        {% endif %}
        """
    print(execute_query(ctx, query, name=name))
