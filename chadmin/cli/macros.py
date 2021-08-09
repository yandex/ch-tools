from click import command, pass_context

from . import execute_query


@command('macros')
@pass_context
def list_macros_command(ctx):
    print(execute_query(ctx, "SELECT * FROM system.macros"))
