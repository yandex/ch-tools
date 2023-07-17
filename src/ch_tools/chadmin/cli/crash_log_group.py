from click import group, option, pass_context

from ch_tools.chadmin.cli import get_cluster_name
from ch_tools.chadmin.internal.utils import execute_query


@group("crash-log")
def crash_log_group():
    """
    Commands for retrieving information from system.crash_log.
    """
    pass


@crash_log_group.command("list")
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Get log records from all hosts in the cluster.",
)
@pass_context
def list_crashes_command(ctx, on_cluster):
    cluster = get_cluster_name(ctx) if on_cluster else None
    query_str = """
        SELECT
        {% if cluster %}
            hostName() "host",
        {% endif %}
            event_time,
            signal,
            thread_id,
            query_id,
            '\n' || arrayStringConcat(trace_full, '\n') AS trace,
            version
        {% if cluster %}
        FROM clusterAllReplicas({{ cluster }}, system.crash_log)
        {% else %}
        FROM system.crash_log
        {% endif %}
        ORDER BY event_time DESC
        """
    print(execute_query(ctx, query_str, cluster=cluster, format_="Vertical"))
