from click import Context

from ch_tools.chadmin.internal.utils import execute_query


def unfreeze_table(
    ctx: Context, database: str, table: str, backup_name: str, dry_run: bool = False
) -> None:
    """
    Perform "ALTER TABLE UNFREEZE".
    """
    timeout = ctx.obj["config"]["clickhouse"]["unfreeze_timeout"]
    query = f"ALTER TABLE `{database}`.`{table}` UNFREEZE WITH NAME '{backup_name}'"
    execute_query(ctx, query, timeout=timeout, echo=True, format_=None, dry_run=dry_run)


def unfreeze_backup(ctx: Context, backup_name: str, dry_run: bool = False) -> None:
    """
    Perform "SYSTEM UNFREEZE".
    """
    timeout = ctx.obj["config"]["clickhouse"]["unfreeze_timeout"]
    query = f"SYSTEM UNFREEZE WITH NAME '{backup_name}'"
    execute_query(ctx, query, timeout=timeout, echo=True, format_=None, dry_run=dry_run)
