from collections import OrderedDict

from click import group, option, pass_context

from ch_tools.chadmin.cli import get_cluster_name
from ch_tools.chadmin.internal.process import list_replicated_fetches
from ch_tools.common.cli.formatting import (
    format_bytes,
    format_float,
    format_percents,
    print_response,
)

FIELD_FORMATTERS = {
    "total_size_bytes_compressed": format_bytes,
    "elapsed": format_float,
    "progress": format_percents,
}


@group("replicated-fetch")
def replicated_fetch_group():
    """Commands to manage fetches (retrieve information from system.replicated_fetches)."""
    pass


@replicated_fetch_group.command(name="list")
@option(
    "-d", "--database", help="Filter in merges to output by the specified database."
)
@option("-t", "--table", help="Filter in merges to output by the specified table.")
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Get merges from all hosts in the cluster.",
)
@option(
    "-l",
    "--limit",
    type=int,
    default=1000,
    help="Limit the max number of objects in the output.",
)
@pass_context
def list_command(ctx, on_cluster, limit, **kwargs):
    """List executing fetches."""

    def _table_formatter(fetch):
        return OrderedDict(
            (
                ("database", fetch["database"]),
                ("table", fetch["table"]),
                ("result_part", fetch["result_part_name"]),
                ("elapsed", fetch["elapsed"]),
                ("progress", fetch["progress"]),
                ("source_replica", fetch["source_replica_hostname"]),
                ("total_size", fetch["total_size_bytes_compressed"]),
            )
        )

    cluster = get_cluster_name(ctx) if on_cluster else None

    merges = list_replicated_fetches(ctx, cluster=cluster, limit=limit, **kwargs)

    print_response(
        ctx,
        merges,
        default_format="table",
        table_formatter=_table_formatter,
        field_formatters=FIELD_FORMATTERS,
    )
