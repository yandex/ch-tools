import os
from collections import OrderedDict
from typing import Any

from cloup import Choice, Context, argument, group, option, option_group, pass_context
from cloup.constraints import (
    If,
    IsSet,
    RequireAtLeast,
    accept_none,
    constraint,
    require_all,
)

from ch_tools.chadmin.internal.table import (
    attach_table,
    delete_detached_table,
    delete_table,
    detach_table,
    get_table,
    list_table_columns,
    list_tables,
    materialize_ttl,
)
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.cli.formatting import format_bytes, print_response
from ch_tools.common.clickhouse.config import get_cluster_name

FIELD_FORMATTERS = {
    "disk_size": format_bytes,
    "uncompressed_size": format_bytes,
}


@group("table")
def table_group():
    """Commands to manage tables."""
    pass


@table_group.command("get")
@argument("database_name", metavar="DATABASE")
@argument("table_name", metavar="TABLE")
@option(
    "--active",
    "--active-parts",
    "active_parts",
    is_flag=True,
    help="Account only active data parts.",
)
@pass_context
def get_command(ctx, database_name, table_name, active_parts):
    """
    Get table.
    """
    table = get_table(ctx, database_name, table_name, active_parts=active_parts)
    print_response(
        ctx,
        table,
        default_format="yaml",
        field_formatters=FIELD_FORMATTERS,
    )


@table_group.command("list")
@option(
    "-d",
    "--database",
    "database_pattern",
    help="Filter in tables to output by the specified database name pattern."
    " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
)
@option(
    "--exclude-database",
    "exclude_database_pattern",
    help="Filter out tables to output by the specified database name pattern."
    " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
)
@option(
    "-t",
    "--table",
    "table_pattern",
    help="Filter in tables to output by the specified table name."
    " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
)
@option(
    "--exclude-table",
    "exclude_table_pattern",
    help="Filter out tables to output by the specified table name."
    " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
)
@option(
    "--engine",
    "engine_pattern",
    help="Filter in tables to output by the specified engine."
    " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
)
@option(
    "--exclude-engine",
    "exclude_engine_pattern",
    help="Filter out tables to output by the specified engine."
    " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
)
@option(
    "--read-only",
    "is_readonly",
    is_flag=True,
    help="Filter in tables in read-only state only.",
)
@option(
    "--active",
    "--active-parts",
    "active_parts",
    is_flag=True,
    help="Account only active data parts.",
)
@option(
    "--order-by",
    type=Choice(["size", "parts", "rows"]),
    help="Sorting order.",
)
@option(
    "-l",
    "--limit",
    type=int,
    default=1000,
    help="Limit the max number of objects in the output.",
)
@pass_context
def list_command(ctx, **kwargs):
    """
    List tables.
    """

    def _table_formatter(item):
        return OrderedDict(
            (
                ("database", item["database"]),
                ("name", item["name"]),
                ("disk_size", item["disk_size"]),
                ("partitions", item["partitions"]),
                ("parts", item["parts"]),
                ("rows", item["rows"]),
                ("metadata_mtime", item["metadata_modification_time"]),
                ("engine", item["engine"]),
            )
        )

    tables = list_tables(ctx, **kwargs)
    print_response(
        ctx,
        tables,
        default_format="table",
        table_formatter=_table_formatter,
        field_formatters=FIELD_FORMATTERS,
    )


@table_group.command("columns")
@argument("database_name", metavar="DATABASE")
@argument("table_name", metavar="TABLE")
@pass_context
def columns_command(ctx, database_name, table_name):
    """
    Describe columns for table.
    """
    table_columns = list_table_columns(ctx, database_name, table_name)
    print_response(
        ctx,
        table_columns,
        default_format="table",
        field_formatters=FIELD_FORMATTERS,
    )


@table_group.command("delete")
@argument("database_name", metavar="DATABASE", required=False)
@argument("table_name", metavar="TABLE", required=False)
@option_group(
    "Table selection options",
    option(
        "-a",
        "--all",
        "_all",
        is_flag=True,
        help="Filter in all tables.",
    ),
    option(
        "-d",
        "--database",
        "database_pattern",
        help="Filter in tables to delete by the specified database name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-database",
        "exclude_database_pattern",
        help="Filter out tables to delete by the specified database name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "-t",
        "--table",
        "table_pattern",
        help="Filter in tables to delete by the specified table name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-table",
        "exclude_table_pattern",
        help="Filter out tables to delete by the specified table name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--engine",
        "engine_pattern",
        help="Filter in tables to delete by the specified engine pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-engine",
        "exclude_engine_pattern",
        help="Filter out tables to delete by the specified engine pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--read-only",
        "is_readonly",
        is_flag=True,
        help="Filter in tables in read-only state only.",
    ),
    constraint=If(IsSet("detached"), then=accept_none, else_=RequireAtLeast(1)),
)
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Delete tables on all hosts of the cluster.",
)
@option(
    "--sync/--async",
    "sync_mode",
    default=True,
    help="Enable/Disable synchronous query execution.",
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@option(
    "--detached",
    is_flag=True,
    help="Delete detached tables (with nonreplicated engine).",
)
@constraint(
    If(IsSet("detached"), then=require_all),
    [
        "database_name",
        "table_name",
        "sync_mode",
    ],
)
@constraint(
    If(IsSet("detached"), then=accept_none),
    [
        "on_cluster",
        "dry_run",
    ],
)
@pass_context
def delete_command(
    ctx,
    _all,
    on_cluster,
    sync_mode,
    dry_run,
    detached,
    database_name,
    table_name,
    **kwargs,
):
    """
    Delete one or several tables.
    """
    cluster = get_cluster_name(ctx) if on_cluster else None

    if detached:
        delete_detached_table(
            ctx,
            database_name=database_name,
            table_name=table_name,
        )
        return

    for t in list_tables(ctx, **kwargs):
        delete_table(
            ctx,
            database_name=t["database"],
            table_name=t["name"],
            cluster=cluster,
            sync_mode=sync_mode,
            echo=True,
            dry_run=dry_run,
        )


@table_group.command("recreate")
@option_group(
    "Table selection options",
    option(
        "-a",
        "--all",
        "_all",
        is_flag=True,
        help="Filter in all tables.",
    ),
    option(
        "-d",
        "--database",
        "database_pattern",
        help="Filter in tables to recreate by the specified database name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-database",
        "exclude_database_pattern",
        help="Filter out tables to recreate by the specified database name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "-t",
        "--table",
        "table_pattern",
        help="Filter in tables to recreate by the specified table name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-table",
        "exclude_table_pattern",
        help="Filter out tables to recreate by the specified table name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--engine",
        "engine_pattern",
        help="Filter in tables to recreate by the specified engine pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-engine",
        "exclude_engine_pattern",
        help="Filter out tables to recreate by the specified engine pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--read-only",
        "is_readonly",
        is_flag=True,
        help="Filter in tables in read-only state only.",
    ),
    constraint=RequireAtLeast(1),
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def recreate_command(ctx, dry_run, **kwargs):
    """
    Recreate one or several tables.
    """
    for t in list_tables(ctx, **kwargs):
        delete_table(
            ctx,
            database_name=t["database"],
            table_name=t["name"],
            echo=True,
            dry_run=dry_run,
        )
        execute_query(
            ctx,
            t["create_table_query"],
            echo=True,
            format_=None,
            dry_run=dry_run,
        )


@table_group.command("detach")
@option_group(
    "Table selection options",
    option(
        "-a",
        "--all",
        "_all",
        is_flag=True,
        help="Filter in all tables.",
    ),
    option(
        "-d",
        "--database",
        "database_pattern",
        help="Filter in tables to detach by the specified database name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-database",
        "exclude_database_pattern",
        help="Filter out tables to detach by the specified database name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "-t",
        "--table",
        "table_pattern",
        help="Filter in tables to detach by the specified table name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-table",
        "exclude_table_pattern",
        help="Filter out tables to detach by the specified table name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--engine",
        "engine_pattern",
        help="Filter in tables to detach by the specified engine pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-engine",
        "exclude_engine_pattern",
        help="Filter out tables to detach by the specified engine pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--read-only",
        "is_readonly",
        is_flag=True,
        help="Filter in tables in read-only state only.",
    ),
    constraint=RequireAtLeast(1),
)
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Detach tables on all hosts of the cluster.",
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def detach_command(
    ctx,
    _all,
    on_cluster,
    dry_run,
    **kwargs,
):
    """
    Detach one or several tables.
    """
    cluster = get_cluster_name(ctx) if on_cluster else None

    for t in list_tables(ctx, **kwargs):
        detach_table(
            ctx,
            database_name=t["database"],
            table_name=t["name"],
            cluster=cluster,
            echo=True,
            dry_run=dry_run,
        )


@table_group.command("reattach")
@option_group(
    "Table selection options",
    option(
        "-a",
        "--all",
        "_all",
        is_flag=True,
        help="Filter in all tables.",
    ),
    option(
        "-d",
        "--database",
        "database_pattern",
        help="Filter in tables to reattach by the specified database name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-database",
        "exclude_database_pattern",
        help="Filter out tables to reattach by the specified database name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "-t",
        "--table",
        "table_pattern",
        help="Filter in tables to reattach by the specified table name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-table",
        "exclude_table_pattern",
        help="Filter out tables to reattach by the specified table name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--engine",
        "engine_pattern",
        help="Filter in tables to reattach by the specified engine pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-engine",
        "exclude_engine_pattern",
        help="Filter out tables to reattach by the specified engine pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--read-only",
        "is_readonly",
        is_flag=True,
        help="Filter in tables in read-only state only.",
    ),
    constraint=RequireAtLeast(1),
)
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Reattach tables on all hosts of the cluster.",
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def reattach_command(
    ctx,
    _all,
    on_cluster,
    dry_run,
    **kwargs,
):
    """
    Reattach one or several tables.
    """
    cluster = get_cluster_name(ctx) if on_cluster else None

    for t in list_tables(ctx, **kwargs):
        detach_table(
            ctx,
            database_name=t["database"],
            table_name=t["name"],
            cluster=cluster,
            echo=True,
            dry_run=dry_run,
        )
        attach_table(
            ctx,
            database_name=t["database"],
            table_name=t["name"],
            cluster=cluster,
            echo=True,
            dry_run=dry_run,
        )


@table_group.command("attach")
@argument("database_name", metavar="DATABASE")
@argument("table_name", metavar="TABLE")
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Reattach tables on all hosts of the cluster.",
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def attach_command(ctx, database_name, table_name, on_cluster, dry_run):
    """
    Attach table.
    """
    cluster = get_cluster_name(ctx) if on_cluster else None
    attach_table(ctx, database_name, table_name, cluster=cluster, dry_run=dry_run)


@table_group.command("materialize-ttl")
@option_group(
    "Table selection options",
    option(
        "-a",
        "--all",
        "_all",
        is_flag=True,
        help="Filter in all tables.",
    ),
    option(
        "-d",
        "--database",
        "database_pattern",
        help="Filter in tables to materialize TTL by the specified database name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-database",
        "exclude_database_pattern",
        help="Filter out tables to materialize TTL by the specified database name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "-t",
        "--table",
        "table_pattern",
        help="Filter in tables to materialize TTL by the specified table name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-table",
        "exclude_table_pattern",
        help="Filter out tables to materialize TTL by the specified table name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--engine",
        "engine_pattern",
        help="Filter in tables to materialize TTL by the specified engine pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-engine",
        "exclude_engine_pattern",
        help="Filter out tables to materialize TTL by the specified engine pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--read-only",
        "is_readonly",
        is_flag=True,
        help="Filter in tables in read-only state only.",
    ),
    constraint=RequireAtLeast(1),
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def materialize_ttl_command(ctx, _all, dry_run, **kwargs):
    """
    Materialize TTL for one or several tables.
    """
    for t in list_tables(ctx, **kwargs):
        materialize_ttl(
            ctx,
            database_name=t["database"],
            table_name=t["name"],
            echo=True,
            dry_run=dry_run,
        )


@table_group.command("set-flag")
@argument("flag")
@option_group(
    "Table selection options",
    option(
        "-a",
        "--all",
        "_all",
        is_flag=True,
        help="Filter in all tables.",
    ),
    option(
        "-d",
        "--database",
        "database_pattern",
        help="Filter in tables to set the flag by the specified database name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-database",
        "exclude_database_pattern",
        help="Filter out tables to set the flag by the specified database name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "-t",
        "--table",
        "table_pattern",
        help="Filter in tables to set the flag by the specified table name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-table",
        "exclude_table_pattern",
        help="Filter out tables to set the flag by the specified table name pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--engine",
        "engine_pattern",
        help="Filter in tables to set the flag by the specified engine pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--exclude-engine",
        "exclude_engine_pattern",
        help="Filter out tables to set the flag by the specified engine pattern."
        " The value can be either a pattern in the LIKE clause format or a comma-separated list of items to match.",
    ),
    option(
        "--read-only",
        "is_readonly",
        is_flag=True,
        help="Filter in tables in read-only state only.",
    ),
    constraint=RequireAtLeast(1),
)
@option(
    "-v",
    "--verbose",
    type=bool,
    is_flag=True,
    help="Show tables and flag paths.",
)
@pass_context
def set_flag_command(
    ctx: Context,
    _all: bool,
    flag: str,
    verbose: bool,
    **kwargs: Any,
) -> None:
    """
    Create a flag with the specified name inside the data directory of the table.
    """
    tables = list_tables(ctx, **kwargs)
    data_paths = [table["data_paths"][0] for table in tables]
    flag_paths = [os.path.join(data_path, flag) for data_path in data_paths]

    for flag_path in flag_paths:
        with open(flag_path, "a", encoding="utf-8") as _:
            pass

    if verbose:
        for table_, flag_path in zip(tables, flag_paths):
            logging.info("{}: {}", table_["table"], flag_path)
