from collections import OrderedDict
from typing import Any

from click import Context
from cloup import group, option, pass_context

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.process import list_moves
from ch_tools.common.cli.formatting import format_bytes, format_float, print_response
from ch_tools.common.clickhouse.config import get_cluster_name

FIELD_FORMATTERS = {
    "part_size": format_bytes,
    "elapsed": format_float,
}


@group("move", cls=Chadmin)
def move_group() -> None:
    """Commands to manage moves (retrieve information from system.moves)."""
    pass


@move_group.command("list")
@option(
    "-d",
    "--database",
    help="Filter in moves to output by the specified database.",
)
@option(
    "-t",
    "--table",
    help="Filter in moves to output by the specified table.",
)
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Get moves from all hosts in the cluster.",
)
@option(
    "-l",
    "--limit",
    type=int,
    default=1000,
    help="Limit the max number of objects in the output.",
)
@pass_context
def list_command(ctx: Context, on_cluster: bool, limit: int, **kwargs: Any) -> None:
    """List executing merges."""

    def _table_formatter(item: Any) -> OrderedDict:
        return OrderedDict(
            (
                ("database", item["database"]),
                ("table", item["table"]),
                ("elapsed", item["elapsed"]),
                ("target_disk", item["target_disk_name"]),
                ("target_path", item["target_disk_path"]),
                ("part_name", item["part_name"]),
                ("part_size", item["part_size"]),
            )
        )

    cluster = get_cluster_name(ctx) if on_cluster else None

    print_response(
        ctx,
        list_moves(ctx, cluster=cluster, limit=limit, **kwargs),
        default_format="table",
        table_formatter=_table_formatter,
        field_formatters=FIELD_FORMATTERS,
    )
