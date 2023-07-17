from ch_tools.chadmin.internal.utils import execute_query


def attach_partition(ctx, database, table, partition_id, dry_run=False):
    """
    Attach the specified table partition.
    """
    query = f"ALTER TABLE `{database}`.`{table}` ATTACH PARTITION ID '{partition_id}'"
    execute_query(ctx, query, timeout=300, format_=None, echo=True, dry_run=dry_run)


def detach_partition(ctx, database, table, partition_id, dry_run=False):
    """
    Detach the specified table partition.
    """
    query = f"ALTER TABLE `{database}`.`{table}` DETACH PARTITION ID '{partition_id}'"
    execute_query(ctx, query, timeout=300, format_=None, echo=True, dry_run=dry_run)


def drop_partition(ctx, database, table, partition_id, dry_run=False):
    """
    Drop the specified table partition.
    """
    query = f"ALTER TABLE `{database}`.`{table}` DROP PARTITION ID '{partition_id}'"
    execute_query(ctx, query, timeout=300, format_=None, echo=True, dry_run=dry_run)


def optimize_partition(ctx, database, table, partition_id, dry_run=False):
    """
    Optimize the specified table partition.
    """
    query = f"OPTIMIZE TABLE `{database}`.`{table}` PARTITION ID '{partition_id}'"
    execute_query(ctx, query, timeout=300, format_=None, echo=True, dry_run=dry_run)


def materialize_ttl_in_partition(ctx, database, table, partition_id, dry_run=False):
    """
    Materialize TTL for the specified table partition.
    """
    query = f"ALTER TABLE `{database}`.`{table}` MATERIALIZE TTL IN PARTITION ID '{partition_id}'"
    execute_query(ctx, query, timeout=300, format_=None, echo=True, dry_run=dry_run)
