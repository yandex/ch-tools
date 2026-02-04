"""
Database management utilities for ClickHouse.

Provides functions for listing databases with statistics and checking database existence.
Supports filtering by database name patterns, engine types, and active parts.
"""

from typing import Any, Optional, Union

from click import Context

from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common.clickhouse.client.query_output_format import OutputFormat


def list_databases(
    ctx: Context,
    database: Optional[str] = None,
    exclude_database_pattern: Optional[str] = None,
    engine_pattern: Optional[str] = None,
    exclude_engine_pattern: Optional[str] = None,
    active_parts: Optional[bool] = None,
    format_: str = "JSON",
) -> Union[Any, dict]:
    query = """
        SELECT
            database,
            engine,
            tables,
            formatReadableSize(bytes_on_disk) "disk_size",
            partitions,
            parts,
            rows
        FROM (
            SELECT
                name "database",
                engine
            FROM system.databases
        ) q1
        ALL LEFT JOIN (
            SELECT
                database,
                count() "tables",
                sum(bytes_on_disk) "bytes_on_disk",
                sum(partitions) "partitions",
                sum(parts) "parts",
                sum(rows) "rows"
            FROM (
                SELECT
                    database,
                    name "table"
                FROM system.tables
            ) q2_1
            ALL LEFT JOIN (
                SELECT
                    database,
                    table,
                    uniq(partition) "partitions",
                    count() "parts",
                    sum(rows) "rows",
                    sum(bytes_on_disk) "bytes_on_disk"
                FROM system.parts
        {% if active_parts %}
                WHERE active
        {% endif %}
                GROUP BY database, table
            ) q2_2
            USING database, table
            GROUP BY database
        ) q2
        USING database
        {% if database %}
        WHERE database {{ format_str_match(database) }}
        {% else %}
        WHERE database NOT IN ('information_schema', 'INFORMATION_SCHEMA')
        {% endif %}
        {% if exclude_database_pattern %}
          AND database NOT {{ format_str_match(exclude_database_pattern) }}
        {% endif %}
        {% if engine_pattern %}
          AND engine {{ format_str_match(engine_pattern) }}
        {% endif %}
        {% if exclude_engine_pattern %}
          AND engine NOT {{ format_str_match(exclude_engine_pattern) }}
        {% endif %}
        ORDER BY database
        """
    res = execute_query(
        ctx,
        query,
        database=database,
        exclude_database_pattern=exclude_database_pattern,
        engine_pattern=engine_pattern,
        exclude_engine_pattern=exclude_engine_pattern,
        active_parts=active_parts,
        format_=format_,
    )
    return res["data"] if format_ == "JSON" else res


def is_database_exists(ctx: Context, database_name: str) -> bool:
    """Check if database exists in ClickHouse."""
    query = """
        SELECT 1 FROM system.databases WHERE database='{{ database_name }}'
    """
    rows = execute_query(
        ctx, query, database_name=database_name, format_=OutputFormat.JSON
    )
    return len(rows["data"]) == 1
