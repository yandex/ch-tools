from cloud.mdb.clickhouse.tools.chadmin.internal.utils import execute_query


def attach_part(ctx, database, table, part_name, dry_run=False):
    """
    Attach the specified data part.
    """
    query = f"ALTER TABLE `{database}`.`{table}` ATTACH PART '{part_name}'"
    execute_query(ctx, query, timeout=300, format=None, echo=True, dry_run=dry_run)


def detach_part(ctx, database, table, part_name, dry_run=False):
    """
    Detach the specified data part.
    """
    query = f"ALTER TABLE `{database}`.`{table}` DETACH PART '{part_name}'"
    execute_query(ctx, query, timeout=300, format=None, echo=True, dry_run=dry_run)


def move_part(ctx, database, table, part_name, new_disk_name, dry_run=False):
    """
    Move the specified data part.
    """
    query = f"ALTER TABLE `{database}`.`{table}` MOVE PART '{part_name}' TO DISK '{new_disk_name}'"
    execute_query(ctx, query, timeout=300, format=None, echo=True, dry_run=dry_run)


def drop_part(ctx, database, table, part_name, dry_run=False):
    """
    Drop the specified data part.
    """
    query = f'ALTER TABLE `{database}`.`{table}` DROP PART \'{part_name}\''
    execute_query(ctx, query, timeout=300, format=None, echo=True, dry_run=dry_run)


def drop_detached_part(ctx, database, table, part_name, dry_run=False):
    """
    Drop the specified detached data part.
    """
    query = f'ALTER TABLE `{database}`.`{table}` DROP DETACHED PART \'{part_name}\''
    execute_query(ctx, query, timeout=300, format=None, echo=True, dry_run=dry_run)


def get_parts(
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
    limit=None,
):
    """
    Get data parts.
    """
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
        WHERE database != 'system'
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
        limit=limit,
        format='JSON',
    )['data']


def get_detached_parts(
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
):
    """
    Get detached data parts.
    """
    query = """
        SELECT
            database,
            table,
            partition_id,
            name,
            disk "disk_name",
            reason
        FROM system.detached_parts
        {% if database -%}
        WHERE database {{ format_str_match(database) }}
        {% else -%}
        WHERE database != 'system'
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
        format='JSON',
    )['data']
