import json
import re
from typing import Any, Dict, List, Optional

from click import Context

from ch_tools.chadmin.internal.clickhouse_disks import remove_from_ch_disk
from ch_tools.chadmin.internal.system import get_version
from ch_tools.chadmin.internal.utils import execute_query, get_remote_table_for_hosts
from ch_tools.common.clickhouse.client.clickhouse_client import clickhouse_client
from ch_tools.common.clickhouse.client.query import Query


def list_parts(
    ctx: Context,
    *,
    database: Optional[str] = None,
    table: Optional[str] = None,
    partition_id: Optional[str] = None,
    min_partition_id: Optional[str] = None,
    max_partition_id: Optional[str] = None,
    part_name: Optional[str] = None,
    disk_name: Optional[str] = None,
    level: Optional[int] = None,
    min_level: Optional[int] = None,
    max_level: Optional[int] = None,
    min_size: Optional[int] = None,
    max_size: Optional[int] = None,
    active: Optional[bool] = None,
    order_by: Optional[str] = None,
    limit: Optional[int] = None,
    use_part_list_from_json: Optional[str] = None,
    remote_replica: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    List data parts.
    """
    if use_part_list_from_json:
        return read_and_validate_parts_from_json(use_part_list_from_json)["data"]

    order_by = {
        "size": "bytes_on_disk DESC",
        "rows": "rows DESC",
        None: "database, table, name",
    }[order_by]

    parts_table = "system.parts"
    sensitive_args = {}
    if remote_replica:
        parts_table = get_remote_table_for_hosts(ctx, parts_table, [remote_replica])
        ch_client = clickhouse_client(ctx)
        sensitive_args = {"user_password": ch_client.password or ""}

    query = Query(
        """
        SELECT
            database,
            table,
            engine,
            partition_id,
            name,
            part_type,
            active,
            disk_name,
            path,
            min_time,
            max_time,
            rows,
            bytes_on_disk,
            modification_time,
            delete_ttl_info_min,
            delete_ttl_info_max,
            move_ttl_info.expression,
            move_ttl_info.min,
            move_ttl_info.max,
            default_compression_codec,
            recompression_ttl_info.expression,
            recompression_ttl_info.min,
            recompression_ttl_info.max,
            group_by_ttl_info.expression,
            group_by_ttl_info.min,
            group_by_ttl_info.max,
            rows_where_ttl_info.expression,
            rows_where_ttl_info.min,
            rows_where_ttl_info.max,
            projections
        FROM {{ parts_table }}
        {% if database -%}
        WHERE database {{ format_str_match(database) }}
        {% else -%}
        WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
        {% endif -%}
        {% if partition_id -%}
          AND partition_id {{ format_str_match(partition_id) }}
        {% endif -%}
        {% if min_partition_id -%}
          AND partition_id >= '{{ min_partition_id }}'
        {% endif -%}
        {% if max_partition_id -%}
          AND partition_id <= '{{ max_partition_id }}'
        {% endif -%}
        {% if part_name -%}
          AND name {{ format_str_match(part_name) }}
        {% endif -%}
        {% if table -%}
          AND table {{ format_str_match(table) }}
        {% endif -%}
        {% if disk_name -%}
          AND disk_name {{ format_str_match(disk_name) }}
        {% endif -%}
        {% if level is not none -%}
          AND level = {{ level }}
        {% endif -%}
        {% if min_level is not none -%}
          AND level >= {{ min_level }}
        {% endif -%}
        {% if max_level is not none -%}
          AND level <= {{ max_level }}
        {% endif -%}
        {% if min_size is not none -%}
          AND bytes_on_disk >= {{ min_size }}
        {% endif -%}
        {% if max_size is not none -%}
          AND bytes_on_disk <= {{ max_size }}
        {% endif -%}
        {% if active -%}
          AND active
        {% endif -%}
        ORDER BY database, table, name
        {% if limit -%}
        LIMIT {{ limit }}
        {% endif -%}
        """,
        sensitive_args=sensitive_args,
    )
    return execute_query(
        ctx,
        query,
        database=database,
        table=table,
        parts_table=parts_table,
        partition_id=partition_id,
        min_partition_id=min_partition_id,
        max_partition_id=max_partition_id,
        part_name=part_name,
        disk_name=disk_name,
        level=level,
        min_level=min_level,
        max_level=max_level,
        min_size=min_size,
        max_size=max_size,
        active=active,
        order_by=order_by,
        limit=limit,
        format_="JSON",
    )["data"]


def list_detached_parts(
    ctx: Context,
    *,
    database: Optional[str] = None,
    table: Optional[str] = None,
    partition_id: Optional[str] = None,
    min_partition_id: Optional[str] = None,
    max_partition_id: Optional[str] = None,
    part_name: Optional[str] = None,
    disk_name: Optional[str] = None,
    level: Optional[int] = None,
    min_level: Optional[int] = None,
    max_level: Optional[int] = None,
    reason: Optional[str] = None,
    limit: Optional[int] = None,
    use_part_list_from_json: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    List detached data parts.
    """
    if use_part_list_from_json:
        return read_and_validate_parts_from_json(use_part_list_from_json)["data"]

    query = """
        SELECT
            database,
            table,
            partition_id,
            name,
            disk "disk_name",
        {% if version_ge('23.1') -%}
            path,
            bytes_on_disk,
        {% endif -%}
            reason
        FROM system.detached_parts
        {% if database -%}
        WHERE database {{ format_str_match(database) }}
        {% else -%}
        WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
        {% endif -%}
        {% if partition_id -%}
          AND partition_id {{ format_str_match(partition_id) }}
        {% endif -%}
        {% if min_partition_id -%}
          AND partition_id >= '{{ min_partition_id }}'
        {% endif -%}
        {% if max_partition_id -%}
          AND partition_id <= '{{ max_partition_id }}'
        {% endif -%}
        {% if part_name -%}
          AND name {{ format_str_match(part_name) }}
        {% endif -%}
        {% if table -%}
          AND table {{ format_str_match(table) }}
        {% endif -%}
        {% if disk_name -%}
          AND disk {{ format_str_match(disk_name) }}
        {% endif -%}
        {% if level is not none -%}
          AND level = {{ level }}
        {% endif -%}
        {% if min_level is not none -%}
          AND level >= {{ min_level }}
        {% endif -%}
        {% if max_level is not none -%}
          AND level <= {{ max_level }}
        {% endif -%}
        {% if reason is not none -%}
          AND reason {{ format_str_match(reason) }}
        {% endif -%}
        ORDER BY database, table, name
        {% if limit -%}
        LIMIT {{ limit }}
        {% endif -%}
        """
    return execute_query(
        ctx,
        query,
        database=database,
        table=table,
        partition_id=partition_id,
        min_partition_id=min_partition_id,
        max_partition_id=max_partition_id,
        part_name=part_name,
        disk_name=disk_name,
        level=level,
        min_level=min_level,
        max_level=max_level,
        reason=reason,
        limit=limit,
        format_="JSON",
    )["data"]


def get_disks(ctx: Context) -> Dict[str, Dict[str, str]]:
    """
    Get disks.
    """
    query = """
        SELECT name, path, type FROM system.disks
        """
    disks = execute_query(
        ctx,
        query,
        format_="JSON",
    )["data"]

    return {disk["name"]: disk for disk in disks}


def attach_part(
    ctx: Context,
    database: str,
    table: str,
    part_name: str,
    dry_run: bool = False,
) -> None:
    """
    Attach the specified data part.
    """
    timeout = ctx.obj["config"]["clickhouse"]["alter_table_timeout"]
    query = f"ALTER TABLE `{database}`.`{table}` ATTACH PART '{part_name}'"
    execute_query(ctx, query, timeout=timeout, format_=None, echo=True, dry_run=dry_run)


def detach_part(
    ctx: Context,
    database: str,
    table: str,
    part_name: str,
    dry_run: bool = False,
) -> None:
    """
    Detach the specified data part.
    """
    timeout = ctx.obj["config"]["clickhouse"]["alter_table_timeout"]
    query = f"ALTER TABLE `{database}`.`{table}` DETACH PART '{part_name}'"
    execute_query(ctx, query, timeout=timeout, format_=None, echo=True, dry_run=dry_run)


def move_part(
    ctx: Context,
    database: str,
    table: str,
    part_name: str,
    new_disk_name: str,
    sync_mode: bool = True,
    dry_run: bool = False,
) -> None:
    """
    Move the specified data part.
    """
    timeout = ctx.obj["config"]["clickhouse"]["alter_table_timeout"]

    settings = {}
    if sync_mode is False:
        settings["alter_move_to_space_execute_async"] = 1

    query = f"ALTER TABLE `{database}`.`{table}` MOVE PART '{part_name}' TO DISK '{new_disk_name}'"
    execute_query(
        ctx,
        query,
        settings=settings,
        timeout=timeout,
        format_=None,
        echo=True,
        dry_run=dry_run,
    )


def drop_part(
    ctx: Context,
    database: str,
    table: str,
    part_name: str,
    dry_run: bool = False,
) -> None:
    """
    Drop the specified data part.
    """
    timeout = ctx.obj["config"]["clickhouse"]["alter_table_timeout"]
    query = f"ALTER TABLE `{database}`.`{table}` DROP PART '{part_name}'"
    execute_query(ctx, query, timeout=timeout, format_=None, echo=True, dry_run=dry_run)


def drop_detached_part(
    ctx: Context,
    database: str,
    table: str,
    part_name: str,
    dry_run: bool = False,
) -> None:
    """
    Drop the specified detached data part.
    """
    timeout = ctx.obj["config"]["clickhouse"]["alter_table_timeout"]
    query = f"ALTER TABLE `{database}`.`{table}` DROP DETACHED PART '{part_name}'"
    execute_query(ctx, query, timeout=timeout, format_=None, echo=True, dry_run=dry_run)


def drop_detached_part_from_disk(
    ctx: Context,
    disk: Dict[str, str],
    path: str,
    dry_run: bool = False,
) -> None:
    """
    Drop the specified detached data part using clickhouse-disks.
    """
    # removeprefix() is not allowed for Python < 3.9
    assert path.startswith(disk["path"])
    path_on_disk = path[len(disk["path"]) :]

    remove_from_ch_disk(disk["name"], path_on_disk, get_version(ctx), dry_run=dry_run)


def list_part_log(
    ctx: Context,
    cluster: Optional[str] = None,
    database: Optional[str] = None,
    table: Optional[str] = None,
    partition: Optional[str] = None,
    part: Optional[str] = None,
    min_date: Optional[str] = None,
    max_date: Optional[str] = None,
    min_time: Optional[str] = None,
    max_time: Optional[str] = None,
    event_type: Optional[str] = None,
    exclude_event_type: Optional[str] = None,
    failed: Optional[bool] = None,
    order_by: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    order_by = {
        "size": "size_in_bytes DESC",
        "rows": "rows DESC",
        "peak_memory_usage": "peak_memory_usage DESC",
        "time": "event_time DESC",
        None: "event_time DESC",
    }[order_by]

    query = """
        SELECT
        {% if cluster %}
             hostName() "host",
        {% endif %}
             event_time,
             event_type,
             merge_reason,
             duration_ms,
             database,
             table,
             partition_id,
             part_name,
             part_type,
             disk_name,
             rows,
             size_in_bytes,
             merged_from,
             read_rows,
             read_bytes,
             peak_memory_usage,
             exception
        {% if cluster %}
        FROM clusterAllReplicas({{ cluster }}, system.part_log)
        {% else %}
        FROM system.part_log
        {% endif %}
        {% if database %}
        WHERE database {{ format_str_match(database) }}
        {% else %}
        WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
        {% endif %}
        {% if table %}
          AND table {{ format_str_match(table) }}
        {% endif %}
        {% if partition %}
          AND partition_id {{ format_str_match(partition) }}
        {% endif %}
        {% if part %}
          AND part_name {{ format_str_match(part) }}
        {% endif %}
        {% if min_date %}
          AND event_date >= toDate('{{ min_date }}')
        {% elif min_time %}
          AND event_date >= toDate('{{ min_time }}')
        {% endif %}
        {% if max_date %}
          AND event_date <= toDate('{{ max_date }}')
        {% elif max_time %}
          AND event_date <= toDate('{{ max_time }}')
        {% endif %}
        {% if not min_date and not max_date and not min_time and not max_time %}
          AND event_date = today()
        {% endif %}
        {% if min_time %}
          AND event_time - INTERVAL duration_ms/1000 second >= toDateTime('{{ min_time }}')
        {% endif %}
        {% if max_time %}
          AND event_time <= toDateTime('{{ max_time }}')
        {% endif %}
        {% if event_type %}
          AND event_type::text {{ format_str_match(event_type) }}
        {% endif %}
        {% if exclude_event_type %}
          AND event_type::text NOT {{ format_str_match(exclude_event_type) }}
        {% endif %}
        {% if failed is true -%}
          AND exception != ''
        {% elif failed is false -%}
          AND exception = ''
        {% endif -%}
        ORDER BY {{ order_by }}
        {% if limit %}
        LIMIT {{ limit }}
        {% endif %}
        """
    return execute_query(
        ctx,
        query,
        cluster=cluster,
        database=database,
        table=table,
        partition=partition,
        part=part,
        min_date=min_date,
        max_date=max_date,
        min_time=min_time,
        max_time=max_time,
        event_type=event_type,
        exclude_event_type=exclude_event_type,
        order_by=order_by,
        failed=failed,
        limit=limit,
        format_="JSON",
    )["data"]


def read_and_validate_parts_from_json(json_path: str) -> Dict[str, Any]:
    base_exception_str = "Incorrect json file, there are no {corrupted_section}. Use the JSON format for ch query to get correct format."
    with open(json_path, "r", encoding="utf-8") as json_file:
        json_obj = json.load(json_file)
        if "data" not in json_obj:
            raise ValueError(base_exception_str.format("data section"))

        part_list = json_obj["data"]
        for p in part_list:
            if "database" not in p:
                raise ValueError(base_exception_str.format("database"))
            if "table" not in p:
                raise ValueError(base_exception_str.format("table"))
            if "name" not in p:
                raise ValueError(base_exception_str.format("name"))
    return json_obj


def part_has_suffix(part_name: str) -> bool:
    return bool(re.match(r".*_try[1-9]$", part_name))
