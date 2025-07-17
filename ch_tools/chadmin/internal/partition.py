from click import Context

from ch_tools.chadmin.internal.utils import execute_query


def attach_partition(
    ctx: Context, database: str, table: str, partition_id: str, dry_run: bool = False
) -> None:
    """
    Attach the specified table partition.
    """
    query = f"ALTER TABLE `{database}`.`{table}` ATTACH PARTITION ID '{partition_id}'"
    _execute_query(ctx, query, dry_run)


def detach_partition(
    ctx: Context, database: str, table: str, partition_id: str, dry_run: bool = False
) -> None:
    """
    Detach the specified table partition.
    """
    query = f"ALTER TABLE `{database}`.`{table}` DETACH PARTITION ID '{partition_id}'"
    _execute_query(ctx, query, dry_run)


def drop_partition(
    ctx: Context, database: str, table: str, partition_id: str, dry_run: bool = False
) -> None:
    """
    Drop the specified table partition.
    """
    query = f"ALTER TABLE `{database}`.`{table}` DROP PARTITION ID '{partition_id}'"
    _execute_query(ctx, query, dry_run)


def optimize_partition(
    ctx: Context, database: str, table: str, partition_id: str, dry_run: bool = False
) -> None:
    """
    Optimize the specified table partition.
    """
    query = f"OPTIMIZE TABLE `{database}`.`{table}` PARTITION ID '{partition_id}'"
    _execute_query(ctx, query, dry_run)


def materialize_ttl_in_partition(
    ctx: Context, database: str, table: str, partition_id: str, dry_run: bool = False
) -> None:
    """
    Materialize TTL for the specified table partition.
    """
    query = f"ALTER TABLE `{database}`.`{table}` MATERIALIZE TTL IN PARTITION ID '{partition_id}'"
    _execute_query(ctx, query, dry_run)


def _execute_query(ctx: Context, query: str, dry_run: bool) -> None:
    timeout = ctx.obj["config"]["clickhouse"]["alter_table_timeout"]
    execute_query(ctx, query, timeout=timeout, format_=None, echo=True, dry_run=dry_run)
