import json

from click import ClickException

from ch_tools.chadmin.internal.part import attach_part
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.chadmin.internal.zookeeper import check_zk_node
from ch_tools.common import logging
from ch_tools.common.clickhouse.client.error import ClickhouseError

RESTORE_STATE_FILE_PATH = "/tmp/restore_replica_parts.json"


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
    zookeeper_path=None,
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
        {% if zookeeper_path -%}
            AND zookeeper_path = '{{ zookeeper_path }}'
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
        zookeeper_path=zookeeper_path,
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


def system_table_drop_replica_by_zk_path(ctx, replica, table_zk_path, dry_run=False):
    """
    Perform "SYSTEM DROP REPLICA <replica> ZKPATH <table_zk_paht>" query.
    """
    timeout = ctx.obj["config"]["clickhouse"]["drop_replica_timeout"]
    query = f"SYSTEM DROP REPLICA '{replica}' FROM ZKPATH '{table_zk_path}'"

    execute_query(ctx, query, timeout=timeout, echo=True, dry_run=dry_run, format_=None)


def system_table_drop_replica(ctx, replica, database, table, dry_run=False):
    """
    Perform "SYSTEM DROP REPLICA <replica> FROM TABLE <database.table>" query.
    """
    timeout = ctx.obj["config"]["clickhouse"]["drop_replica_timeout"]
    query = f"SYSTEM DROP REPLICA '{replica}' FROM TABLE `{database}`.`{table}`"
    execute_query(ctx, query, timeout=timeout, echo=True, dry_run=dry_run, format_=None)


def list_active_parts(
    ctx,
    database_name,
    table_name,
):
    """
    List active parts of a table.
    """
    query = "SELECT database, table, name FROM system.parts WHERE database = '{{database_name}}' AND table = '{{table_name}}' AND active = 1"
    return execute_query(
        ctx,
        query,
        database_name=database_name,
        table_name=table_name,
        format_="JSON",
    )["data"]


def no_replicas_in_zookeeper(ctx, database_name, table_name):
    replicas_path = (
        get_table_replica(ctx, database_name, table_name)["zookeeper_path"]
        + "/replicas"
    )
    return not bool(check_zk_node(ctx, replicas_path))


def table_is_readonly(ctx, database_name, table_name):
    return bool(get_table_replica(ctx, database_name, table_name)["is_readonly"])


def restore_replica(ctx, database, table, cluster, dry_run):
    # TODO: remove attach when restore is fixed in ClickHouse
    is_first_replica = no_replicas_in_zookeeper(ctx, database, table)
    parts_to_attach = (
        list_active_parts(ctx, database, table) if is_first_replica else None
    )
    try:
        restore_table_replica(
            ctx,
            database,
            table,
            cluster=cluster,
            dry_run=dry_run,
        )
    except ClickhouseError as e:
        msg = e.response.text.strip()
        if "BAD_ARGUMENTS" in msg:
            logging.warning(
                'Failed to restore replica with error "{}", attempting to recover by restarting replica',
                msg,
            )
            restart_table_replica(
                ctx,
                database,
                table,
                cluster=cluster,
                dry_run=dry_run,
            )
        elif "NO_ZOOKEEPER" in msg or "Session expired" in msg:
            _dump_json_with_parts(parts_to_attach)
            raise
        elif not is_first_replica:
            raise
        else:
            restart_table_replica(
                ctx,
                database,
                table,
                cluster=cluster,
                dry_run=dry_run,
            )
            if table_is_readonly(ctx, database, table):
                _dump_json_with_parts(parts_to_attach)
                raise
            logging.warning(
                'Replica was restored, but some part failed to attach with error "{}". Will attach other parts',
                msg,
            )

            for part in parts_to_attach:
                try:
                    attach_part(ctx, database, table, part["name"], dry_run)
                except ClickhouseError as e_:
                    msg = e_.response.text.strip()
                    if "NO_ZOOKEEPER" in msg or "Session expired" in msg:
                        _dump_json_with_parts(parts_to_attach)
                        raise


def _dump_json_with_parts(parts_to_attach):
    logging.error(
        """
                Failed to restore replica with, try again when connection to ZooKeeper is restored.
                List of active parts before restore will be saved to "{}" file in case they are detached"
                """,
        RESTORE_STATE_FILE_PATH,
    )
    # "data" key is required in attach command
    data = {"data": parts_to_attach}
    with open(RESTORE_STATE_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f)
