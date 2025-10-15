from typing import Any, Optional

from click import Context

from ch_tools.chadmin.internal.system import match_ch_version
from ch_tools.chadmin.internal.utils import execute_query


def system_database_drop_replica(
    ctx: Context, database_zk_path: str, replica: str, dry_run: bool = False
) -> None:
    """
    Perform "SYSTEM DROP DATABASE REPLICA" query.
    """
    timeout = ctx.obj["config"]["clickhouse"]["drop_replica_timeout"]
    query = f"SYSTEM DROP DATABASE REPLICA '{replica}' FROM ZKPATH '{database_zk_path}'"
    execute_query(ctx, query, timeout=timeout, echo=True, dry_run=dry_run, format_=None)


def list_database_replicas(
    ctx: Context,
    *,
    database_name: Optional[str] = None,
    database_pattern: Optional[str] = None,
    exclude_database_pattern: Optional[str] = None,
    verbose: bool = False,
    limit: Optional[int] = None,
) -> list[dict[str, Any]]:
    """
    List replicas of replicated databases.
    """
    if not match_ch_version(ctx, "25.9"):
        raise RuntimeError("This command requires version 25.9 of ClickHouse")

    query = """
        SELECT
            database,
            zookeeper_path,
            replica_name,
            replica_path
        {% if verbose -%}
            is_readonly,
            max_log_ptr,
            shard_name,
            log_ptr,
            total_replicas,
            zookeeper_exception,
            is_session_expired,
        {% endif -%}
        FROM system.database_replicas
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
        verbose=verbose,
        limit=limit,
        format_="JSON",
    )["data"]
