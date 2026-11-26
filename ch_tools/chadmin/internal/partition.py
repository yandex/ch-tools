from click import Context

from ch_tools.chadmin.internal.utils import execute_query


def attach_partition(
    ctx: Context, database: str, table: str, partition_id: str, dry_run: bool = False
) -> None:
    """
    Attach the specified table partition.
    """
    _execute_partition_id(
        ctx,
        "ALTER TABLE {{ db|bind_identifier }}.{{ tbl|bind_identifier }} ATTACH PARTITION ID {{ part|bind }}",
        database,
        table,
        partition_id,
        dry_run,
    )


def detach_partition(
    ctx: Context, database: str, table: str, partition_id: str, dry_run: bool = False
) -> None:
    """
    Detach the specified table partition.
    """
    _execute_partition_id(
        ctx,
        "ALTER TABLE {{ db|bind_identifier }}.{{ tbl|bind_identifier }} DETACH PARTITION ID {{ part|bind }}",
        database,
        table,
        partition_id,
        dry_run,
    )


def drop_partition(
    ctx: Context, database: str, table: str, partition_id: str, dry_run: bool = False
) -> None:
    """
    Drop the specified table partition.
    """
    _execute_partition_id(
        ctx,
        "ALTER TABLE {{ db|bind_identifier }}.{{ tbl|bind_identifier }} DROP PARTITION ID {{ part|bind }}",
        database,
        table,
        partition_id,
        dry_run,
    )


def optimize_partition(
    ctx: Context, database: str, table: str, partition_id: str, dry_run: bool = False
) -> None:
    """
    Optimize the specified table partition.
    """
    _execute_partition_id(
        ctx,
        "OPTIMIZE TABLE {{ db|bind_identifier }}.{{ tbl|bind_identifier }} PARTITION ID {{ part|bind }}",
        database,
        table,
        partition_id,
        dry_run,
    )


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
    part = "tuple()" if partition == "tuple()" else "{{ part|bind }}"
    query = (
        "ALTER TABLE {{ db|bind_identifier }}.{{ tbl|bind_identifier }} MOVE PARTITION "
        + part
        + " TO TABLE {{ dst_db|identifier }}.{{ dst_tbl|identifier }}"
    )
    execute_query(
        ctx,
        query,
        db=src_database,
        tbl=src_table,
        dst_db=dst_database,
        dst_tbl=dst_table,
        part=partition,
        timeout=ctx.obj["config"]["clickhouse"]["alter_table_timeout"],
        format_=None,
        echo=True,
        dry_run=dry_run,
    )


def materialize_ttl_in_partition(
    ctx: Context, database: str, table: str, partition_id: str, dry_run: bool = False
) -> None:
    """
    Materialize TTL for the specified table partition.
    """
    _execute_partition_id(
        ctx,
        "ALTER TABLE {{ db|bind_identifier }}.{{ tbl|bind_identifier }} MATERIALIZE TTL IN PARTITION ID {{ part|bind }}",
        database,
        table,
        partition_id,
        dry_run,
    )


def _execute_partition_id(
    ctx: Context,
    query: str,
    database: str,
    table: str,
    partition_id: str,
    dry_run: bool,
) -> None:
    timeout = ctx.obj["config"]["clickhouse"]["alter_table_timeout"]
    execute_query(
        ctx,
        query,
        db=database,
        tbl=table,
        part=partition_id,
        timeout=timeout,
        format_=None,
        echo=True,
        dry_run=dry_run,
    )
