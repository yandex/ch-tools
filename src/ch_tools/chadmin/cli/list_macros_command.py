from chtools.chadmin.internal.utils import execute_query
from click import command, pass_context


@command("macros")
@pass_context
def list_macros_command(ctx):
    """
    Show macros.
    """
    print(execute_query(ctx, "SELECT * FROM system.macros"))
