import json

from ch_tools.chadmin.internal.utils import execute_query


def list_parts(
    ctx,
    *,
    database=None,
    table=None,
    partition_id=None,
    min_partition_id=None,
    max_partition_id=None,
    part_name=None,
    disk_name=None,
    level=None,
    min_level=None,
    max_level=None,
    min_size=None,
    max_size=None,
    active=None,
    order_by=None,
    limit=None,
    use_part_list_from_json=None,
):
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

    query = """
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
        FROM system.parts
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
        min_size=min_size,
        max_size=max_size,
        active=active,
        order_by=order_by,
        limit=limit,
        format_="JSON",
    )["data"]


def list_detached_parts(
    ctx,
    *,
    database=None,
    table=None,
    partition_id=None,
    min_partition_id=None,
    max_partition_id=None,
    part_name=None,
    disk_name=None,
    level=None,
    min_level=None,
    max_level=None,
    reason=None,
    limit=None,
    use_part_list_from_json=None,
):
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


def attach_part(ctx, database, table, part_name, dry_run=False):
    """
    Attach the specified data part.
    """
    timeout = ctx.obj["config"]["clickhouse"]["alter_table_timeout"]
    query = f"ALTER TABLE `{database}`.`{table}` ATTACH PART '{part_name}'"
    execute_query(ctx, query, timeout=timeout, format_=None, echo=True, dry_run=dry_run)


def detach_part(ctx, database, table, part_name, dry_run=False):
    """
    Detach the specified data part.
    """
    timeout = ctx.obj["config"]["clickhouse"]["alter_table_timeout"]
    query = f"ALTER TABLE `{database}`.`{table}` DETACH PART '{part_name}'"
    execute_query(ctx, query, timeout=timeout, format_=None, echo=True, dry_run=dry_run)


def move_part(
    ctx,
    database,
    table,
    part_name,
    new_disk_name,
    sync_mode=True,
    dry_run=False,
):
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


def drop_part(ctx, database, table, part_name, dry_run=False):
    """
    Drop the specified data part.
    """
    timeout = ctx.obj["config"]["clickhouse"]["alter_table_timeout"]
    query = f"ALTER TABLE `{database}`.`{table}` DROP PART '{part_name}'"
    execute_query(ctx, query, timeout=timeout, format_=None, echo=True, dry_run=dry_run)


def drop_detached_part(ctx, database, table, part_name, dry_run=False):
    """
    Drop the specified detached data part.
    """
    timeout = ctx.obj["config"]["clickhouse"]["alter_table_timeout"]
    query = f"ALTER TABLE `{database}`.`{table}` DROP DETACHED PART '{part_name}'"
    execute_query(ctx, query, timeout=timeout, format_=None, echo=True, dry_run=dry_run)


def list_part_log(
    ctx,
    cluster=None,
    database=None,
    table=None,
    partition=None,
    part=None,
    min_date=None,
    max_date=None,
    min_time=None,
    max_time=None,
    failed=None,
    order_by=None,
    limit=None,
):
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
        order_by=order_by,
        failed=failed,
        limit=limit,
        format_="JSON",
    )["data"]


def read_and_validate_parts_from_json(json_path):
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
