from click import Context

from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common.clickhouse.client.sql import qualified_name, quote_string


def attach_partition(
    ctx: Context, database: str, table: str, partition_id: str, dry_run: bool = False
) -> None:
    """
    Attach the specified table partition.
    """
    query = f"ALTER TABLE {qualified_name(database, table)} ATTACH PARTITION ID {quote_string(partition_id)}"
    _execute_query(ctx, query, dry_run)


def detach_partition(
    ctx: Context, database: str, table: str, partition_id: str, dry_run: bool = False
) -> None:
    """
    Detach the specified table partition.
    """
    query = f"ALTER TABLE {qualified_name(database, table)} DETACH PARTITION ID {quote_string(partition_id)}"
    _execute_query(ctx, query, dry_run)


def drop_partition(
    ctx: Context, database: str, table: str, partition_id: str, dry_run: bool = False
) -> None:
    """
    Drop the specified table partition.
    """
    query = f"ALTER TABLE {qualified_name(database, table)} DROP PARTITION ID {quote_string(partition_id)}"
    _execute_query(ctx, query, dry_run)


def optimize_partition(
    ctx: Context, database: str, table: str, partition_id: str, dry_run: bool = False
) -> None:
    """
    Optimize the specified table partition.
    """
    query = f"OPTIMIZE TABLE {qualified_name(database, table)} PARTITION ID {quote_string(partition_id)}"
    _execute_query(ctx, query, dry_run)


def move_partition(
    ctx: Context,
    src_database: str,
    src_table: str,
    partition: str,
    dst_database: str,
    dst_table: str,
    dry_run: bool = False,
) -> None:
    """
    Move the specified table partition between tables.
    """
    partition_expr = quote_string(partition) if partition != "tuple()" else partition
    query = f"ALTER TABLE {qualified_name(src_database, src_table)} MOVE PARTITION {partition_expr} TO TABLE {qualified_name(dst_database, dst_table)}"
    _execute_query(ctx, query, dry_run)


def materialize_ttl_in_partition(
    ctx: Context, database: str, table: str, partition_id: str, dry_run: bool = False
) -> None:
    """
    Materialize TTL for the specified table partition.
    """
    query = f"ALTER TABLE {qualified_name(database, table)} MATERIALIZE TTL IN PARTITION ID {quote_string(partition_id)}"
    _execute_query(ctx, query, dry_run)


def _execute_query(ctx: Context, query: str, dry_run: bool) -> None:
    timeout = ctx.obj["config"]["clickhouse"]["alter_table_timeout"]
    execute_query(ctx, query, timeout=timeout, format_=None, echo=True, dry_run=dry_run)
