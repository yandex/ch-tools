from click import Context, group, option, pass_context

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.clickhouse.config import get_cluster_name


@group("crash-log", cls=Chadmin)
def crash_log_group() -> None:
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
def list_crashes_command(ctx: Context, on_cluster: bool) -> None:
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
    logging.info(execute_query(ctx, query_str, cluster=cluster, format_="Vertical"))
