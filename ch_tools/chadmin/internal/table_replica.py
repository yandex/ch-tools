from click import ClickException

from ch_tools.chadmin.internal.utils import execute_query


def get_table_replica(ctx, database_name, table_name):
    """
    Get replica of replicated table.
    """
    replicas = list_table_replicas(
        ctx,
        database_name=database_name,
        table_name=table_name,
        verbose=True,
    )

    if not replicas:
        raise ClickException(
            f"Replicated table `{database_name}`.`{table_name}` not found."
        )

    return replicas[0]


def list_table_replicas(
    ctx,
    *,
    database_name=None,
    database_pattern=None,
    exclude_database_pattern=None,
    table_name=None,
    table_pattern=None,
    exclude_table_pattern=None,
    is_readonly=None,
    verbose=False,
    limit=None,
):
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
        {% if not verbose -%}
            replica_path
        {% else -%}
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
        {% endif -%}
        FROM system.replicas
        WHERE true
        {% if database_name -%}
            AND database = '{{ database_name }}'
        {% endif -%}
        {% if database_pattern -%}
            AND database {{ format_str_match(database_pattern) }}
        {% endif -%}
        {% if exclude_database_pattern -%}
            AND database NOT {{ format_str_match(exclude_database_pattern) }}
        {% endif -%}
        {% if table_name -%}
            AND table = '{{ table_name }}'
        {% endif -%}
        {% if table_pattern -%}
            AND table {{ format_str_match(table_pattern) }}
        {% endif -%}
        {% if exclude_table_pattern -%}
            AND table NOT {{ format_str_match(exclude_table_pattern) }}
        {% endif -%}
        {% if is_readonly -%}
           AND is_readonly
        {% endif -%}
        {% if limit is not none -%}
        LIMIT {{ limit }}
        {% endif -%}
        """
    return execute_query(
        ctx,
        query,
        database_name=database_name,
        database_pattern=database_pattern,
        exclude_database_pattern=exclude_database_pattern,
        table_name=table_name,
        table_pattern=table_pattern,
        exclude_table_pattern=exclude_table_pattern,
        is_readonly=is_readonly,
        verbose=verbose,
        limit=limit,
        format_="JSON",
    )["data"]


def restart_table_replica(
    ctx,
    database_name,
    table_name,
    *,
    cluster=None,
    dry_run=False,
):
    """
    Perform "SYSTEM RESTART REPLICA" for the specified replicated table.
    """
    timeout = ctx.obj["config"]["clickhouse"]["restart_replica_timeout"]
    query = f"SYSTEM RESTART REPLICA `{database_name}`.`{table_name}`"
    if cluster:
        query += f" ON CLUSTER '{cluster}'"
    execute_query(ctx, query, timeout=timeout, echo=True, dry_run=dry_run, format_=None)


def restore_table_replica(
    ctx,
    database_name,
    table_name,
    *,
    cluster=None,
    dry_run=False,
):
    """
    Perform "SYSTEM RESTORE REPLICA" for the specified replicated table.
    """
    timeout = ctx.obj["config"]["clickhouse"]["restore_replica_timeout"]
    query = f"SYSTEM RESTORE REPLICA `{database_name}`.`{table_name}`"
    if cluster:
        query += f" ON CLUSTER '{cluster}'"
    execute_query(ctx, query, timeout=timeout, echo=True, dry_run=dry_run, format_=None)
