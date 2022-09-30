from click import group, option, pass_context
from cloud.mdb.cli.common.parameters import BytesParamType
from cloud.mdb.clickhouse.tools.chadmin.internal.partition import (
    attach_partition,
    detach_partition,
    drop_partition,
    materialize_ttl_in_partition,
    optimize_partition,
)

from cloud.mdb.clickhouse.tools.chadmin.internal.utils import execute_query


@group('partition')
def partition_group():
    """Partition management commands."""
    pass


@partition_group.command(name='list')
@option('--database', help='Filter in partitions to output by the specified database.')
@option('-t', '--table', help='Filter in partitions to output by the specified table.')
@option('--partition', 'partition_id', help='Filter in partitions to output by the specified partition.')
@option('--min-partition', 'min_partition_id')
@option('--max-partition', 'max_partition_id')
@option('--min-date')
@option('--max-date')
@option('--min-parts', '--min-part-count', 'min_part_count')
@option('--max-parts', '--max-part-count', 'max_part_count')
@option('--min-size', type=BytesParamType())
@option('--max-size', type=BytesParamType())
@option('--disk', 'disk_name', help='Filter in partitions to output by the specified disk.')
@option('--merging', is_flag=True)
@option('--detached', is_flag=True, help='Show detached partitions instead of attached.')
@option('--active', '--active-parts', 'active_parts', is_flag=True, help='Account only active data parts.')
@option('-l', '--limit', type=int, help='Limit the max number of objects in the output.')
@pass_context
def list_partitions_command(ctx, **kwargs):
    """List partitions."""
    print(get_partitions(ctx, format='PrettyCompact', **kwargs))


@partition_group.command(name='attach')
@option('--database', help='Filter in partitions to attach by the specified database.')
@option('-t', '--table', help='Filter in partitions to attach by the specified table.')
@option('--partition', 'partition_id', help='Filter in partitions to attach by the specified partition.')
@option('-a', '--all', is_flag=True, help='Attach all partitions.')
@option(
    '-n', '--dry-run', is_flag=True, default=False, help='Enable dry run mode and do not perform any modifying actions.'
)
@pass_context
def attach_partitions_command(ctx, dry_run, all, database, table, partition_id):
    """Attach one or several partitions."""
    if not any((all, database, table, partition_id)):
        ctx.fail('At least one of --all, --database, --table, --partition options must be specified.')

    partitions = get_partitions(ctx, database, table, partition_id=partition_id, detached=True, format='JSON')['data']
    for p in partitions:
        attach_partition(ctx, p['database'], p['table'], p['partition_id'], dry_run=dry_run)


@partition_group.command(name='detach')
@option('--database', help='Filter in partitions to detach by the specified database.')
@option('-t', '--table', help='Filter in partitions to detach by the specified table.')
@option('--partition', 'partition_id', help='Filter in partitions to detach by the specified partition.')
@option('--disk', 'disk_name', help='Filter in partitions to detach by the specified disk.')
@option('-a', '--all', is_flag=True, help='Detach all partitions.')
@option(
    '-n', '--dry-run', is_flag=True, default=False, help='Enable dry run mode and do not perform any modifying actions.'
)
@pass_context
def detach_partitions_command(ctx, dry_run, all, database, table, partition_id, disk_name):
    """Detach one or several partitions."""
    if not any((all, database, table, partition_id)):
        ctx.fail('At least one of --all, --database, --table, --partition options must be specified.')

    partitions = get_partitions(ctx, database, table, partition_id=partition_id, disk_name=disk_name, format='JSON')[
        'data'
    ]
    for p in partitions:
        detach_partition(ctx, p['database'], p['table'], p['partition_id'], dry_run=dry_run)


@partition_group.command(name='reattach')
@option('--database', help='Filter in partitions to reattach by the specified database.')
@option('-t', '--table', help='Filter in partitions to reattach by the specified table.')
@option('--partition', 'partition_id', help='Filter in partitions to reattach by the specified partition.')
@option('--min-partition', 'min_partition_id')
@option('--max-partition', 'max_partition_id')
@option('--disk', 'disk_name', help='Filter in partitions to reattach by the specified disk.')
@option('--merging', is_flag=True)
@option('-a', '--all', is_flag=True, help='Reattach all partitions.')
@option('-l', '--limit', type=int, help='Limit the max number of partitions to reaatach in the output.')
@option(
    '-n', '--dry-run', is_flag=True, default=False, help='Enable dry run mode and do not perform any modifying actions.'
)
@pass_context
def reattach_partitions_command(
    ctx, dry_run, all, database, table, partition_id, min_partition_id, max_partition_id, disk_name, merging, limit
):
    """Perform sequential attach and detach of one or several partitions."""
    if not any((all, merging, database, table, partition_id)):
        ctx.fail(
            'At least one of --all, --database, --table, --partition, --min-partition, --max-partition'
            ' and --merging options must be specified.'
        )

    partitions = get_partitions(
        ctx,
        database,
        table,
        partition_id=partition_id,
        min_partition_id=min_partition_id,
        max_partition_id=max_partition_id,
        merging=merging,
        disk_name=disk_name,
        limit=limit,
        format='JSON',
    )['data']
    for p in partitions:
        detach_partition(ctx, p['database'], p['table'], p['partition_id'], dry_run=dry_run)
        attach_partition(ctx, p['database'], p['table'], p['partition_id'], dry_run=dry_run)


@partition_group.command(name='delete')
@option('--database', help='Filter in partitions to delete by the specified database.')
@option('-t', '--table', help='Filter in partitions to delete by the specified table.')
@option('--partition', 'partition_id', help='Filter in partitions to delete by the specified partition.')
@option('--min-partition', 'min_partition_id')
@option('--max-partition', 'max_partition_id')
@option('--min-date')
@option('--max-date')
@option('--disk', 'disk_name', help='Filter in partitions to delete by the specified disk.')
@option(
    '-n', '--dry-run', is_flag=True, default=False, help='Enable dry run mode and do not perform any modifying actions.'
)
@pass_context
def delete_partitions_command(
    ctx, dry_run, database, table, partition_id, min_partition_id, max_partition_id, min_date, max_date, disk_name
):
    """Delete one or several partitions."""
    if not any((database, table, partition_id, min_partition_id, max_partition_id)):
        ctx.fail(
            'At least one of --database, --table, --partition, --min-partition and --max-partition'
            ' options must be specified.'
        )

    partitions = get_partitions(
        ctx,
        database,
        table,
        partition_id=partition_id,
        min_partition_id=min_partition_id,
        max_partition_id=max_partition_id,
        min_date=min_date,
        max_date=max_date,
        disk_name=disk_name,
        format='JSON',
    )['data']
    for p in partitions:
        drop_partition(ctx, p['database'], p['table'], p['partition_id'], dry_run=dry_run)


@partition_group.command(name='optimize')
@option('--database', help='Filter in partitions to optimize by the specified database.')
@option('-t', '--table', help='Filter in partitions to optimize by the specified table.')
@option('--partition', 'partition_id', help='Filter in partitions to optimize by the specified partition.')
@option('--min-partition', 'min_partition_id')
@option('--max-partition', 'max_partition_id')
@option('--min-date')
@option('--max-date')
@option('--disk', 'disk_name', help='Filter in partitions to optimize by the specified disk.')
@option(
    '-n', '--dry-run', is_flag=True, default=False, help='Enable dry run mode and do not perform any modifying actions.'
)
@pass_context
def optimize_partitions_command(
    ctx, dry_run, database, table, partition_id, min_partition_id, max_partition_id, min_date, max_date, disk_name
):
    """Optimize partitions."""
    if not any((database, table, partition_id, min_partition_id, max_partition_id)):
        ctx.fail(
            'At least one of --database, --table, --partition, --min-partition and --max-partition'
            ' options must be specified.'
        )

    for p in get_partitions(
        ctx,
        database,
        table,
        partition_id=partition_id,
        min_partition_id=min_partition_id,
        max_partition_id=max_partition_id,
        min_date=min_date,
        max_date=max_date,
        disk_name=disk_name,
        format='JSON',
    )['data']:
        optimize_partition(ctx, p['database'], p['table'], p['partition_id'], dry_run=dry_run)


@partition_group.command(name='materialize-ttl')
@option('--database', help='Filter in partitions to materialize TTL by the specified database.')
@option('-t', '--table', help='Filter in partitions to materialize TTL by the specified table.')
@option('--partition', 'partition_id', help='Filter in partitions to materialize TTL by the specified partition.')
@option('--min-partition', 'min_partition_id')
@option('--max-partition', 'max_partition_id')
@option('--min-date')
@option('--max-date')
@option('--disk', 'disk_name', help='Filter in partitions to materialize TTL by the specified disk.')
@option(
    '-n', '--dry-run', is_flag=True, default=False, help='Enable dry run mode and do not perform any modifying actions.'
)
@pass_context
def materialize_ttl_command(
    ctx, dry_run, database, table, partition_id, min_partition_id, max_partition_id, min_date, max_date, disk_name
):
    """Materialize TTL."""
    if not any((database, table, partition_id, min_partition_id, max_partition_id)):
        ctx.fail(
            'At least one of --database, --table, --partition, --min-partition and --max-partition'
            ' options must be specified.'
        )

    for p in get_partitions(
        ctx,
        database,
        table,
        partition_id=partition_id,
        min_partition_id=min_partition_id,
        max_partition_id=max_partition_id,
        min_date=min_date,
        max_date=max_date,
        disk_name=disk_name,
        format='JSON',
    )['data']:
        materialize_ttl_in_partition(ctx, p['database'], p['table'], p['partition_id'], dry_run=dry_run)


def get_partitions(
    ctx,
    database,
    table,
    *,
    partition_id=None,
    min_partition_id=None,
    max_partition_id=None,
    min_date=None,
    max_date=None,
    min_part_count=None,
    max_part_count=None,
    min_size=None,
    max_size=None,
    active_parts=None,
    disk_name=None,
    merging=None,
    detached=None,
    limit=None,
    format=None,
):
    if detached:
        query = """
            SELECT
                database,
                table,
                partition_id,
                count() "parts"
            FROM system.detached_parts
            {% if database %}
              WHERE database {{ format_str_match(database) }}
            {% else %}
              WHERE database != 'system'
            {% endif %}
            {% if table %}
              AND table {{ format_str_match(table) }}
            {% endif %}
            GROUP BY database, table, partition_id
            HAVING 1
            {% if partition_id %}
              AND partition_id {{ format_str_match(partition_id) }}
            {% endif %}
            {% if min_partition_id %}
              AND partition_id >= '{{ min_partition_id }}'
            {% endif %}
            {% if max_partition_id %}
              AND partition_id <= '{{ max_partition_id }}'
            {% endif %}
            {% if min_part_count %}
              AND parts >= {{ min_part_count }}
            {% endif %}
            {% if max_part_count %}
              AND parts <= {{ max_part_count }}
            {% endif %}
            ORDER BY database, table, partition_id
            {% if limit %}
            LIMIT {{ limit }}
            {% endif %}
            """
    else:
        query = """
            SELECT
                database,
                table,
                partition_id,
                count() "parts",
                min(min_time) "min_time",
                max(max_time) "max_time",
                arrayStringConcat(groupUniqArray(disk_name), ', ') "disks",
                sum(rows) "rows",
                formatReadableSize(sum(bytes_on_disk)) "bytes"
            FROM system.parts
            {% if database %}
              WHERE database {{ format_str_match(database) }}
            {% else %}
              WHERE database != 'system'
            {% endif %}
            {% if active_parts %}
                AND active
            {% endif %}
            {% if table %}
              AND table {{ format_str_match(table) }}
            {% endif %}
            GROUP BY database, table, partition_id
            HAVING 1
            {% if disk_name %}
               AND has(groupUniqArray(disk_name), '{{ disk_name }}')
            {% endif %}
            {% if partition_id %}
              AND partition_id {{ format_str_match(partition_id) }}
            {% endif %}
            {% if min_partition_id %}
              AND partition_id >= '{{ min_partition_id }}'
            {% endif %}
            {% if max_partition_id %}
              AND partition_id <= '{{ max_partition_id }}'
            {% endif %}
            {% if min_date %}
              AND max_date >= '{{ min_date }}'
            {% endif %}
            {% if max_date %}
              AND min_date <= '{{ max_date }}'
            {% endif %}
            {% if min_size %}
              AND sum(bytes_on_disk) >= '{{ min_size }}'
            {% endif %}
            {% if max_size %}
              AND sum(bytes_on_disk) <= '{{ max_size }}'
            {% endif %}
            {% if merging %}
              AND (database, table, partition_id) IN (
                  SELECT (database, table, partition_id)
                  FROM system.merges
              )
            {% endif %}
            ORDER BY database, table, partition_id
            {% if limit %}
            LIMIT {{ limit }}
            {% endif %}
            """
    return execute_query(
        ctx,
        query,
        database=database,
        table=table,
        partition_id=partition_id,
        min_partition_id=min_partition_id,
        max_partition_id=max_partition_id,
        min_date=min_date,
        max_date=max_date,
        min_part_count=min_part_count,
        max_part_count=max_part_count,
        min_size=min_size,
        max_size=max_size,
        active_parts=active_parts,
        disk_name=disk_name,
        merging=merging,
        limit=limit,
        format=format,
    )


def get_partition_key_type(ctx, database, table):
    """
    Get partition key type.
    """
    query = 'SELECT {partition_key} FROM `{database}`.`{table}` LIMIT 0'.format(
        database=database, table=table, partition_key=get_partition_key(ctx, database, table)
    )
    return execute_query(ctx, query, format='JSON')['meta'][0]['type']


def get_partition_key(ctx, database, table):
    """
    Get partition key.
    """
    query = """
        SELECT partition_key
        FROM system.tables
        WHERE database = '{database}'
          AND name = '{table}'
        """.format(
        database=database, table=table
    )
    return execute_query(ctx, query, format='JSONCompact')['data'][0][0]
