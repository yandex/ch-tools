from click import ClickException

from ch_tools.chadmin.internal.utils import execute_query


def get_table_replica(ctx, database, table):
    """
    Get replica of replicated table.
    """
    replicas = list_table_replicas(ctx, database=database, table=table)

    if not replicas:
        raise ClickException(f"Replicated table `{database}`.`{table}` not found.")

    return replicas[0]


def list_table_replicas(ctx, *, database=None, table=None, limit=None):
    """
    List replicas of replicated tables.
    """
    query = """
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
        WHERE true
        {% if database %}
          AND database {{ format_str_match(database) }}
        {% endif %}
        {% if table %}
          AND table {{ format_str_match(table) }}
        {% endif %}
        {% if limit is not none %}
        LIMIT {{ limit }}
        {% endif %}
        """
    return execute_query(
        ctx,
        query,
        database=database,
        table=table,
        limit=limit,
        format_="JSON",
    )["data"]


def restart_table_replica(ctx, database, table, *, cluster=None):
    """
    Perform "SYSTEM RESTART REPLICA" for the specified replicated table.
    """
    query = f"SYSTEM RESTART REPLICA `{database}`.`{table}`"
    if cluster:
        query += f" ON CLUSTER '{cluster}'"
    execute_query(ctx, query, timeout=300, echo=True, format_=None)


def restore_table_replica(ctx, database, table, *, cluster=None):
    """
    Perform "SYSTEM RESTORE REPLICA" for the specified replicated table.
    """
    query = f"SYSTEM RESTORE REPLICA `{database}`.`{table}`"
    if cluster:
        query += f" ON CLUSTER '{cluster}'"
    execute_query(ctx, query, timeout=600, echo=True, format_=None)
