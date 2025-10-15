from typing import Any, Optional, Union

from click import Context

from ch_tools.chadmin.internal.utils import execute_query


def list_databases(
    ctx: Context,
    database: Optional[str] = None,
    exclude_database: Optional[str] = None,
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
        WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
        {% endif %}
        {% if exclude_database %}
          AND database != '{{ exclude_database }}'
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
        exclude_database=exclude_database,
        engine_pattern=engine_pattern,
        exclude_engine_pattern=exclude_engine_pattern,
        active_parts=active_parts,
        format_=format_,
    )
    if format_ == "JSON":
        return res["data"]
    return res

