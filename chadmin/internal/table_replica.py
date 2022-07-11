from cloud.mdb.clickhouse.tools.chadmin.internal.utils import execute_query
from click import ClickException


def get_table_replica(ctx, database, table):
    """
    Get replica of replicated table.
    """
    query = f"""
        SELECT
            database,
            table,
            engine,
            zookeeper_path,
            replica_name,
            replica_path,
            is_leader,
            can_become_leader,
            is_readonly,
            is_session_expired,
            absolute_delay,
            queue_size,
            inserts_in_queue,
            merges_in_queue,
            part_mutations_in_queue,
            last_queue_update,
            log_pointer,
            log_max_index,
            last_queue_update_exception,
            zookeeper_exception,
            total_replicas,
            active_replicas,
            replica_is_active
        FROM system.replicas
        WHERE database = '{database}'
          AND table = '{table}'
        """
    replicas = execute_query(ctx, query, format='JSON')['data']

    if not replicas:
        raise ClickException(f'Replicated table `{database}`.`{table}` not found.')

    return replicas[0]
