from click import Choice, argument, group, option, pass_context

from ch_tools.chadmin.cli import get_cluster_name
from ch_tools.chadmin.internal.table import (
    attach_table,
    delete_table,
    detach_table,
    get_table,
    list_tables,
    materialize_ttl,
)
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common.cli.formatting import print_response


@group("table")
def table_group():
    """Commands to manage tables."""
    pass


@table_group.command("get")
@argument("database")
@argument("table")
@option(
    "--active",
    "--active-parts",
    "active_parts",
    is_flag=True,
    help="Account only active data parts.",
)
@pass_context
def get_command(ctx, database, table, active_parts):
    """
    Get table.
    """
    table = get_table(ctx, database, table, active_parts=active_parts)
    print_response(ctx, table, default_format="yaml")


@table_group.command("list")
@option(
    "-d", "--database", help="Filter tables to output by the specified database name."
)
@option("-t", "--table", help="Filter in tables to output by the specified table name.")
@option(
    "--exclude-table", help="Filter out tables to output by the specified table name."
)
@option("--engine", help="Filter tables to output by the specified engine.")
@option(
    "--active",
    "--active-parts",
    "active_parts",
    is_flag=True,
    help="Account only active data parts.",
)
@option("-v", "--verbose", is_flag=True, help="Verbose mode.")
@option("--order-by", type=Choice(["size", "parts", "rows"]), help="Sorting order.")
@option(
    "-l",
    "--limit",
    type=int,
    default=1000,
    help="Limit the max number of objects in the output.",
)
@pass_context
def list_command(
    ctx, database, table, exclude_table, engine, active_parts, verbose, order_by, limit
):
    """
    List tables.
    """
    tables = list_tables(
        ctx,
        database=database,
        table=table,
        exclude_table=exclude_table,
        engine=engine,
        active_parts=active_parts,
        verbose=verbose,
        order_by=order_by,
        limit=limit,
    )
    print_response(ctx, tables, default_format="table")


@table_group.command("columns")
@argument("database")
@argument("table")
@pass_context
def columns_command(ctx, database, table):
    """
    Describe columns for table.
    """
    query = """
        SELECT
            name,
            type,
            default_kind,
            default_expression,
            formatReadableSize(data_compressed_bytes) "disk_size",
            formatReadableSize(data_uncompressed_bytes) "uncompressed_size",
            marks_bytes
        FROM system.columns
        WHERE database = '{{ database }}'
          AND table = '{{ table }}'
        """
    print(execute_query(ctx, query, database=database, table=table))


@table_group.command("delete")
@pass_context
@option(
    "-d",
    "--database",
    help="Filter in tables to delete by the specified database name.",
)
@option("-t", "--table", help="Filter in tables to delete by the specified table name.")
@option(
    "--exclude-table", help="Filter out tables to delete by the specified table name."
)
@option("-a", "--all", "all_", is_flag=True, help="Delete all tables.")
@option("--cluster")
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
def delete_command(ctx, dry_run, all_, database, table, exclude_table, cluster):
    """
    Delete one or several tables.
    """
    if not any((all_, database, table)):
        ctx.fail(
            "At least one of --all, --database, --table options must be specified."
        )

    for t in list_tables(
        ctx, database=database, table=table, exclude_table=exclude_table
    ):
        delete_table(
            ctx,
            database=t["database"],
            table=t["table"],
            cluster=cluster,
            echo=True,
            dry_run=dry_run,
        )


@table_group.command("recreate")
@pass_context
@option(
    "-d",
    "--database",
    help="Filter in tables to recreate by the specified database name.",
)
@option(
    "-t", "--table", help="Filter in tables to recreate by the specified table name."
)
@option(
    "--exclude-table", help="Filter out tables to recreate by the specified table name."
)
@option("-a", "--all", "all_", is_flag=True, help="Recreate all tables.")
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
def recreate_command(ctx, dry_run, all_, database, table, exclude_table):
    """
    Recreate one or several tables.
    """
    if not any((all_, database, table)):
        ctx.fail(
            "At least one of --all, --database, --table options must be specified."
        )

    for t in list_tables(
        ctx, database=database, table=table, exclude_table=exclude_table, verbose=True
    ):
        delete_table(
            ctx, database=t["database"], table=t["table"], echo=True, dry_run=dry_run
        )
        execute_query(
            ctx, t["create_table_query"], echo=True, format_=None, dry_run=dry_run
        )


@table_group.command("detach")
@pass_context
@option(
    "-d",
    "--database",
    help="Filter in tables to detach by the specified database name.",
)
@option("-t", "--table", help="Filter in tables to detach by the specified table name.")
@option(
    "--exclude-table", help="Filter out tables to reattach by the specified table name."
)
@option("--engine", help="Filter tables to detach by the specified engine.")
@option("-a", "--all", "all_", is_flag=True, help="Detach all tables.")
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Perform detach queries with ON CLUSTER modificator.",
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
def detach_command(
    ctx, dry_run, all_, database, table, engine, exclude_table, on_cluster
):
    """
    Detach one or several tables.
    """
    if not any((all_, database, table)):
        ctx.fail(
            "At least one of --all, --database, --table options must be specified."
        )

    cluster = get_cluster_name(ctx) if on_cluster else None

    for t in list_tables(
        ctx, database=database, table=table, engine=engine, exclude_table=exclude_table
    ):
        detach_table(
            ctx,
            database=t["database"],
            table=t["table"],
            cluster=cluster,
            echo=True,
            dry_run=dry_run,
        )


@table_group.command("reattach")
@pass_context
@option(
    "-d",
    "--database",
    help="Filter in tables to reattach by the specified database name.",
)
@option(
    "-t", "--table", help="Filter in tables to reattach by the specified table name."
)
@option(
    "--exclude-table", help="Filter out tables to reattach by the specified table name."
)
@option("--engine", help="Filter tables to reattach by the specified engine.")
@option("-a", "--all", "all_", is_flag=True, help="Reattach all tables.")
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Perform attach and detach queries with ON CLUSTER modificator.",
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
def reattach_command(
    ctx, dry_run, all_, database, table, engine, exclude_table, on_cluster
):
    """
    Reattach one or several tables.
    """
    if not any((all_, database, table)):
        ctx.fail(
            "At least one of --all, --database, --table options must be specified."
        )

    cluster = get_cluster_name(ctx) if on_cluster else None

    for t in list_tables(
        ctx, database=database, table=table, engine=engine, exclude_table=exclude_table
    ):
        detach_table(
            ctx,
            database=t["database"],
            table=t["table"],
            cluster=cluster,
            echo=True,
            dry_run=dry_run,
        )
        attach_table(
            ctx,
            database=t["database"],
            table=t["table"],
            cluster=cluster,
            echo=True,
            dry_run=dry_run,
        )


@table_group.command("materialize-ttl")
@pass_context
@option(
    "-d",
    "--database",
    help="Filter in tables to materialize TTL by the specified database name.",
)
@option(
    "-t",
    "--table",
    help="Filter in tables to materialize TTL by the specified table name.",
)
@option(
    "--exclude-table",
    help="Filter out tables to materialize TTL by the specified table name.",
)
@option("-a", "--all", "all_", is_flag=True, help="Materialize TTL for all tables.")
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
def materialize_ttl_command(ctx, dry_run, all_, database, table, exclude_table):
    """
    Materialize TTL for one or several tables.
    """
    if not any((all_, database, table)):
        ctx.fail(
            "At least one of --all, --database, --table options must be specified."
        )

    for t in list_tables(
        ctx, database=database, table=table, exclude_table=exclude_table
    ):
        materialize_ttl(
            ctx, database=t["database"], table=t["table"], echo=True, dry_run=dry_run
        )
