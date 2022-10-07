from cloud.mdb.clickhouse.tools.chadmin.internal.utils import execute_query


def attach_part(ctx, database, table, part_name, dry_run=False):
    """
    Attach the specified data part.
    """
    query = f"ALTER TABLE `{database}`.`{table}` ATTACH PART '{part_name}'"
    execute_query(ctx, query, timeout=300, format=None, echo=True, dry_run=dry_run)


def detach_part(ctx, database, table, part_name, dry_run=False):
    """
    Detach the specified data part.
    """
    query = f"ALTER TABLE `{database}`.`{table}` DETACH PART '{part_name}'"
    execute_query(ctx, query, timeout=300, format=None, echo=True, dry_run=dry_run)


def move_part(ctx, database, table, part_name, new_disk_name, dry_run=False):
    """
    Move the specified data part.
    """
    query = f"ALTER TABLE `{database}`.`{table}` MOVE PART '{part_name}' TO DISK '{new_disk_name}'"
    execute_query(ctx, query, timeout=300, format=None, echo=True, dry_run=dry_run)


def drop_part(ctx, database, table, part_name, dry_run=False):
    """
    Drop the specified data part.
    """
    query = f'ALTER TABLE `{database}`.`{table}` DROP PART \'{part_name}\''
    execute_query(ctx, query, timeout=300, format=None, echo=True, dry_run=dry_run)


def drop_detached_part(ctx, database, table, part_name, dry_run=False):
    """
    Drop the specified detached data part.
    """
    query = f'ALTER TABLE `{database}`.`{table}` DROP DETACHED PART \'{part_name}\''
    execute_query(ctx, query, timeout=300, format=None, echo=True, dry_run=dry_run)
