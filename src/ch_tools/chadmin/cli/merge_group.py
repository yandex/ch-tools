from collections import OrderedDict

from click import group, option, pass_context

from ch_tools.chadmin.cli import get_cluster_name
from ch_tools.chadmin.internal.process import list_merges
from ch_tools.common.cli.formatting import (
    format_bytes,
    format_float,
    format_percents,
    print_response,
)

FIELD_FORMATTERS = {
    "total_size_bytes_compressed": format_bytes,
    "bytes_read_uncompressed": format_bytes,
    "bytes_written_uncompressed": format_bytes,
    "memory_usage": format_bytes,
    "elapsed": format_float,
    "progress": format_percents,
}


@group("merge")
def merge_group():
    """Commands to manage merges (retrieve information from system.merges)."""
    pass


@merge_group.command(name="list")
@option(
    "-d", "--database", help="Filter in merges to output by the specified database."
)
@option("-t", "--table", help="Filter in merges to output by the specified table.")
@option("--mutation", "is_mutation", is_flag=True)
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
    """List executing merges."""

    def _table_formatter(merge):
        if merge["is_mutation"]:
            merge_type = "mutation"
        else:
            merge_type = f"{merge['merge_type']} {merge['merge_algorithm']} merge"
        return OrderedDict(
            (
                ("database", merge["database"]),
                ("table", merge["table"]),
                ("result_part", merge["result_part_name"]),
                ("source_parts", "\n".join(merge["source_part_names"])),
                ("type", merge_type),
                ("elapsed", merge["elapsed"]),
                ("progress", merge["progress"]),
                ("total_size", merge["total_size_bytes_compressed"]),
                ("memory_usage", merge["memory_usage"]),
            )
        )

    cluster = get_cluster_name(ctx) if on_cluster else None

    merges = list_merges(ctx, cluster=cluster, limit=limit, **kwargs)

    print_response(
        ctx,
        merges,
        default_format="table",
        table_formatter=_table_formatter,
        field_formatters=FIELD_FORMATTERS,
    )
