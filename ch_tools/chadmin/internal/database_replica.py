from ch_tools.chadmin.internal.utils import execute_query


def system_database_drop_replica(ctx, database_zk_path, replica, dry_run=False):
    """
    Perform "SYSTEM DROP DATABASE REPLICA" query.
    """
    timeout = ctx.obj["config"]["clickhouse"]["drop_replica_timeout"]
    query = f"SYSTEM DROP DATABASE REPLICA '{replica}' FROM ZKPATH '{database_zk_path}'"
    execute_query(ctx, query, timeout=timeout, echo=True, dry_run=dry_run, format_=None)
