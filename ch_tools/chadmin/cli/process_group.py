from cloup import Choice, argument, group, option, option_group, pass_context
from cloup.constraints import RequireAtLeast

from ch_tools.chadmin.internal.process import get_process, kill_process, list_processes
from ch_tools.chadmin.internal.utils import format_query
from ch_tools.common.cli.formatting import print_response
from ch_tools.common.clickhouse.config import get_cluster_name

FIELD_FORMATTERS = {
    "query": format_query,
}


@group("process")
def process_group():
    """
    Commands to manage processes.
    """
    pass


@process_group.command("get")
@argument("query_id")
@pass_context
def get_process_command(ctx, query_id):
    """
    Get process.
    """
    process = get_process(ctx, query_id)
    print_response(
        ctx, process, default_format="yaml", field_formatters=FIELD_FORMATTERS
    )


@process_group.command("list")
@option("-u", "--user")
@option("-U", "--exclude-user")
@option("--query")
@option("-v", "--verbose", is_flag=True, help="Verbose mode.")
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Get records from all hosts in the cluster.",
)
@option(
    "--order-by",
    type=Choice(["elapsed", "memory_usage"]),
    default="elapsed",
    help="Sorting order.",
)
@option(
    "-l", "--limit", type=int, help="Limit the max number of objects in the output."
)
@pass_context
def list_processes_command(
    ctx, user, exclude_user, query, verbose, on_cluster, order_by, limit
):
    """
    List processes.
    """
    cluster = get_cluster_name(ctx) if on_cluster else None

    processes = list_processes(
        ctx,
        user=user,
        exclude_user=exclude_user,
        query_pattern=query,
        cluster=cluster,
        limit=limit,
        order_by=order_by,
        verbose=verbose,
    )

    print_response(
        ctx, processes, default_format="yaml", field_formatters=FIELD_FORMATTERS
    )


@process_group.command("kill")
@option_group(
    "Process selection options",
    option("-a", "--all", "_all", is_flag=True, help="Kill all processes."),
    option("-q", "--query", "query_id"),
    option("-u", "--user"),
    option("-U", "--exclude-user"),
    constraint=RequireAtLeast(1),
)
@pass_context
def kill_process_command(ctx, _all, query_id, user, exclude_user):
    """
    Kill one or several processes using "KILL QUERY" query.
    """
    kill_process(ctx, query_id=query_id, user=user, exclude_user=exclude_user)
