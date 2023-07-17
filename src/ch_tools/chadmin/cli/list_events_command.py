from chtools.chadmin.internal.utils import execute_query
from click import command, pass_context


@command("events")
@pass_context
def list_events_command(ctx):
    """
    Show metrics from system.events.
    """
    print(execute_query(ctx, "SELECT * FROM system.events"))
