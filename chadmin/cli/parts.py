from click import group, option, pass_context

from cloud.mdb.clickhouse.tools.chadmin.cli import execute_query


@group('part')
def part_group():
    """Part management commands."""
    pass


@part_group.command(name='list')
@option('--database')
@option('-t', '--table')
@option('--partition', 'partition_id')
@option('--part', 'part_name')
@option('--disk', 'disk_name')
@option('--active', is_flag=True)
@option('-v', '--verbose', is_flag=True)
@option('-l', '--limit')
@pass_context
def list_parts_command(ctx, verbose, **kwargs):
    """List data parts."""
    format = 'Vertical' if verbose else 'PrettyCompact'
    print(get_parts(ctx, **kwargs, verbose=verbose, format=format))


@part_group.command(name='delete')
@option('--database')
@option('-t', '--table')
@option('--partition', 'partition_id')
@option('--part', 'part_name')
@option('--disk', 'disk_name')
@option('-l', '--limit')
@option('-k', '--keep-going', is_flag=True,
        help='Do not stop on the first failed command.')
@option('-n', '--dry-run', is_flag=True)
@pass_context
def delete_parts_command(ctx, database, table, partition_id, part_name, disk_name, limit, keep_going, dry_run):
    """Delete one or several data parts."""
    if not any((database, table, partition_id, part_name, disk_name)):
        ctx.fail('At least one of --database, --table, --partition, --part, --disk options must be specified.')

    parts = get_parts(ctx,
                      database=database,
                      table=table,
                      partition_id=partition_id,
                      part_name=part_name,
                      disk_name=disk_name,
                      limit=limit,
                      format='JSON')['data']
    for part in parts:
        try:
            drop_part(ctx, part['database'], part['table'], part['name'], dry_run=dry_run)
        except Exception as e:
            if keep_going:
                print(repr(e))
            else:
                raise


@part_group.command(name='move')
@option('--database')
@option('-t', '--table')
@option('--partition', 'partition_id')
@option('--part', 'part_name')
@option('--disk', 'disk_name')
@option('--new-disk', 'new_disk_name', required=True)
@option('-l', '--limit')
@option('-k', '--keep-going', is_flag=True,
        help='Do not stop on the first failed command.')
@option('-n', '--dry-run', is_flag=True)
@pass_context
def move_parts_command(ctx, database, table, partition_id, part_name, disk_name, new_disk_name, limit, keep_going, dry_run):
    """Move one or several data parts."""
    if not any((database, table, partition_id, part_name, disk_name)):
        ctx.fail('At least one of --database, --table, --partition, --part, --disk options must be specified.')

    parts = get_parts(ctx,
                      database=database,
                      table=table,
                      partition_id=partition_id,
                      part_name=part_name,
                      disk_name=disk_name,
                      active=True,
                      limit=limit,
                      format='JSON')['data']
    for part in parts:
        try:
            move_part(ctx, part['database'], part['table'], part['name'], new_disk_name, dry_run=dry_run)
        except Exception as e:
            if keep_going:
                print(repr(e))
            else:
                raise


def get_parts(ctx,
              database,
              table,
              partition_id=None,
              part_name=None,
              disk_name=None,
              active=None,
              verbose=False,
              limit=None,
              format=None):
    """
    Get data parts.
    """
    query = """
        SELECT
            database,
            table,
        {% if verbose -%}
            engine,
            partition_id,
        {% endif -%}
            name,
            part_type,
            active,
            disk_name,
        {% if verbose -%}
            path,
        {% endif -%}
            min_date,
            max_date,
            rows,
        {% if not verbose -%}
            formatReadableSize(bytes_on_disk) "size"
        {% else -%}
            formatReadableSize(bytes_on_disk) "size",
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
        {% endif -%}
        FROM system.parts
        {% if database -%}
        WHERE database {{ format_str_match(database) }}
        {% else -%}
        WHERE database != 'system'
        {% endif -%}
        {% if partition_id -%}
          AND partition_id {{ format_str_match(partition_id) }}
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
        {% if active -%}
          AND active
        {% endif -%}
        ORDER BY database, table, partition_id
        {% if limit -%}
        LIMIT {{ limit }}
        {% endif -%}
        """
    return execute_query(ctx,
                         query,
                         database=database,
                         table=table,
                         partition_id=partition_id,
                         part_name=part_name,
                         disk_name=disk_name,
                         active=active,
                         verbose=verbose,
                         limit=limit,
                         format=format)


def drop_part(ctx, database, table, part_name, dry_run=False):
    """
    Drop the specified data part.
    """
    query = f'ALTER TABLE `{database}`.`{table}` DROP PART \'{part_name}\''
    execute_query(ctx, query, echo=True, dry_run=dry_run)


def move_part(ctx, database, table, part_name, new_disk_name, dry_run=False):
    """
    Move the specified data part.
    """
    query = f"ALTER TABLE `{database}`.`{table}` MOVE PART '{part_name}' TO DISK '{new_disk_name}'"
    execute_query(ctx, query, echo=True, dry_run=dry_run, timeout=600)
