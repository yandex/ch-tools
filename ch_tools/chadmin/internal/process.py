from typing import Any, Dict, List, Optional

from click import ClickException, Context

from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging


def get_process(ctx: Context, query_id: str) -> Dict[str, Any]:
    """
    Get executing query from system.processes table.
    """
    processes = list_processes(ctx, query_id=query_id, verbose=True)

    if not processes:
        raise ClickException(f"Process {query_id} not found.")

    return processes[0]


def list_processes(
    ctx: Context,
    user: Optional[str] = None,
    exclude_user: Optional[str] = None,
    query_id: Optional[str] = None,
    query_pattern: Optional[str] = None,
    cluster: Optional[str] = None,
    limit: Optional[int] = None,
    order_by: str = "elapsed",
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Get list of executing queries from system.processes table.
    """
    query = """
        SELECT
        {% if cluster %}
             hostName() "host",
        {% endif %}
             query_id,
             elapsed,
             query,
             is_cancelled,
             concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) "read",
             concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) "written",
             formatReadableSize(memory_usage) "memory usage",
             user,
             multiIf(empty(client_name),
                     http_user_agent,
                     concat(client_name, ' ',
                            toString(client_version_major), '.',
                            toString(client_version_minor), '.',
        {% if not verbose %}
                            toString(client_version_patch))) "client"
        {% else %}
                            toString(client_version_patch))) "client",
        ProfileEvents,
        Settings
        {% endif %}
        {% if cluster %}
        FROM clusterAllReplicas({{ cluster }}, system.processes)
        {% else %}
        FROM system.processes
        {% endif %}
        WHERE 1
        {% if user %}
          AND user = '{{ user }}'
        {% endif %}
        {% if exclude_user %}
          AND user != '{{ exclude_user }}'
        {% endif %}
        {% if query_id %}
          AND query_id = '{{ query_id }}'
        {% endif %}
        {% if query_pattern %}
          AND lower(query) LIKE lower('{{ query_pattern }}')
        {% endif %}
        {% if not query_id %}
        ORDER BY {{ order_by }} DESC
        {% endif %}
        {% if limit %}
        LIMIT {{ limit }}
        {% endif %}
        """
    return execute_query(
        ctx,
        query,
        user=user,
        exclude_user=exclude_user,
        query_id=query_id,
        query_pattern=query_pattern,
        cluster=cluster,
        limit=limit,
        verbose=verbose,
        order_by=order_by,
        format_="JSON",
    )["data"]


def kill_process(
    ctx: Context,
    query_id: Optional[str] = None,
    user: Optional[str] = None,
    exclude_user: Optional[str] = None,
) -> None:
    """
    Perform "KILL QUERY".
    """
    query_str = """
        KILL QUERY
        WHERE 1
        {% if user %}
          AND user = '{{ user }}'
        {% endif %}
        {% if exclude_user %}
          AND user != '{{ exclude_user }}'
        {% endif %}
        {% if query_id %}
          AND query_id = '{{ query_id }}'
        {% endif %}
        """
    logging.info(
        execute_query(
            ctx, query_str, query_id=query_id, user=user, exclude_user=exclude_user
        )
    )


def list_merges(
    ctx: Context,
    *,
    database: Optional[str] = None,
    table: Optional[str] = None,
    is_mutation: Optional[bool] = None,
    cluster: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Get list of executing merges from system.merges table.
    """
    query = """
        SELECT
        {% if cluster %}
            hostName() "host",
        {% endif %}
            database,
            table,
            elapsed,
            progress,
            is_mutation,
            merge_type,
            merge_algorithm,
            partition_id,
            num_parts,
            source_part_names,
            result_part_name,
            source_part_paths,
            result_part_path,
            total_size_bytes_compressed,
            total_size_marks,
            bytes_read_uncompressed,
            rows_read,
            bytes_written_uncompressed,
            rows_written,
            columns_written,
            memory_usage,
            thread_id
        {% if cluster %}
        FROM clusterAllReplicas({{ cluster }}, system.merges)
        {% else %}
        FROM system.merges
        {% endif %}
        WHERE 1
        {% if database %}
          AND database {{ format_str_match(database) }}
        {% endif %}
        {% if table %}
          AND table {{ format_str_match(table) }}
        {% endif %}
        {% if is_mutation %}
          AND is_mutation
        {% endif %}
        {% if limit %}
        LIMIT {{ limit }}
        {% endif %}
        """
    return execute_query(
        ctx,
        query,
        database=database,
        table=table,
        is_mutation=is_mutation,
        cluster=cluster,
        limit=limit,
        format_="JSON",
    )["data"]


def list_moves(
    ctx: Context,
    *,
    database: Optional[str] = None,
    table: Optional[str] = None,
    cluster: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Get list of executing moves from system.moves table.
    """
    query = """
        SELECT
        {% if cluster %}
            hostName() "host",
        {% endif %}
            database,
            table,
            elapsed,
            target_disk_name,
            target_disk_path,
            part_name,
            part_size,
            thread_id
        {% if cluster %}
        FROM clusterAllReplicas({{ cluster }}, system.moves)
        {% else %}
        FROM system.moves
        {% endif %}
        WHERE 1
        {% if database %}
          AND database {{ format_str_match(database) }}
        {% endif %}
        {% if table %}
          AND table {{ format_str_match(table) }}
        {% endif %}
        {% if limit %}
        LIMIT {{ limit }}
        {% endif %}
        """
    return execute_query(
        ctx,
        query,
        database=database,
        table=table,
        cluster=cluster,
        limit=limit,
        format_="JSON",
    )["data"]


def list_replicated_fetches(
    ctx: Context,
    *,
    database: Optional[str] = None,
    table: Optional[str] = None,
    cluster: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Get list of executing fetches from system.replicated_fetches table.
    """
    query = """
        SELECT
        {% if cluster %}
            hostName() "host",
        {% endif %}
            database,
            table,
            elapsed,
            progress,
            result_part_name,
            result_part_path,
            partition_id,
            total_size_bytes_compressed,
            bytes_read_compressed,
            source_replica_path,
            source_replica_hostname,
            source_replica_port,
            interserver_scheme,
            URI,
            to_detached,
            thread_id
        {% if cluster %}
        FROM clusterAllReplicas({{ cluster }}, system.replicated_fetches)
        {% else %}
        FROM system.replicated_fetches
        {% endif %}
        WHERE 1
        {% if database %}
          AND database {{ format_str_match(database) }}
        {% endif %}
        {% if table %}
          AND table {{ format_str_match(table) }}
        {% endif %}
        {% if limit %}
        LIMIT {{ limit }}
        {% endif %}
        """
    return execute_query(
        ctx,
        query,
        database=database,
        table=table,
        cluster=cluster,
        limit=limit,
        format_="JSON",
    )["data"]


def stop_merges(
    ctx: Context,
    database: str,
    table: str,
    dry_run: bool = False,
) -> None:
    """
    Stop merges for the specified table.
    """
    query = f"SYSTEM STOP MERGES `{database}`.`{table}`"
    execute_query(ctx, query, timeout=300, format_=None, echo=True, dry_run=dry_run)


def start_merges(
    ctx: Context,
    database: str,
    table: str,
    dry_run: bool = False,
) -> None:
    """
    Start merges for the specified table.
    """
    query = f"SYSTEM START MERGES `{database}`.`{table}`"
    execute_query(ctx, query, timeout=300, format_=None, echo=True, dry_run=dry_run)
