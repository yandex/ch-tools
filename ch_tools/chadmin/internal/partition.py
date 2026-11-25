from click import Context

from ch_tools.chadmin.internal.system import match_ch_version
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common.clickhouse.client.parameters import (
    Identifier,
    Parameter,
    table_parameters,
)


def attach_partition(
    ctx: Context, database: str, table: str, partition_id: str, dry_run: bool = False
) -> None:
    """
    Attach the specified table partition.
    """
    _execute_partition_id(
        ctx,
        "ALTER TABLE {{ db|bind_identifier }}.{{ tbl|bind_identifier }} ATTACH PARTITION ID ",
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
        "ALTER TABLE {{ db|bind_identifier }}.{{ tbl|bind_identifier }} DETACH PARTITION ID ",
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
        "ALTER TABLE {{ db|bind_identifier }}.{{ tbl|bind_identifier }} DROP PARTITION ID ",
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
        "OPTIMIZE TABLE {{ db|bind_identifier }}.{{ tbl|bind_identifier }} PARTITION ID ",
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
    parameters: dict[str, Parameter] = {
        **table_parameters(src_database, src_table),
        "dst_db": Identifier(dst_database),
        "dst_tbl": Identifier(dst_table),
    }
    part_expr = "tuple()"
    if partition != part_expr:
        parameters["part"] = partition
        part_expr = "{part:String}"
    query = (
        "ALTER TABLE {db:Identifier}.{tbl:Identifier} MOVE PARTITION "
        + part_expr
        + " TO TABLE {dst_db:Identifier}.{dst_tbl:Identifier}"
    )
    _execute_query(ctx, query, parameters, dry_run)


def materialize_ttl_in_partition(
    ctx: Context, database: str, table: str, partition_id: str, dry_run: bool = False
) -> None:
    """
    Materialize TTL for the specified table partition.
    """
    _execute_partition_id(
        ctx,
        "ALTER TABLE {{ db|bind_identifier }}.{{ tbl|bind_identifier }} MATERIALIZE TTL IN PARTITION ID ",
        database,
        table,
        partition_id,
        dry_run,
    )


def _execute_partition_id(
    ctx: Context,
    prefix: str,
    database: str,
    table: str,
    partition_id: str,
    dry_run: bool,
) -> None:
    # Older ClickHouse parsers accept only a quoted literal after PARTITION ID.
    suffix = (
        "{{ id|bind }}"
        if not dry_run and match_ch_version(ctx, "24.8")
        else "{{ id|literal }}"
    )
    timeout = ctx.obj["config"]["clickhouse"]["alter_table_timeout"]
    execute_query(
        ctx,
        prefix + suffix,
        db=database,
        tbl=table,
        id=partition_id,
        timeout=timeout,
        format_=None,
        echo=True,
        dry_run=dry_run,
    )


def _execute_query(
    ctx: Context, query: str, parameters: dict[str, Parameter], dry_run: bool
) -> None:
    timeout = ctx.obj["config"]["clickhouse"]["alter_table_timeout"]
    execute_query(
        ctx,
        query,
        parameters=parameters,
        timeout=timeout,
        format_=None,
        echo=True,
        dry_run=dry_run,
    )
