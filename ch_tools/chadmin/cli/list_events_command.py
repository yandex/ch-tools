from click import command, pass_context

from ch_tools.chadmin.internal.utils import execute_query


@command("events")
@pass_context
def list_events_command(ctx):
    """
    Show metrics from system.events.
    """
    print(execute_query(ctx, "SELECT * FROM system.events"))
