from click import group, option, pass_context

from cloud.mdb.clickhouse.tools.chadmin.cli import execute_query


@group('partition')
def partition_group():
    """Partition management commands."""
    pass


@partition_group.command(name='list')
@option('--database')
@option('-t', '--table')
@option('--partition', 'partition_id')
@option('--min-partition')
@option('--max-partition')
@option('--min-date')
@option('--max-date')
@option('--detached', is_flag=True, help='Show detached partitions instead of attached.')
@option('--active-parts', is_flag=True, help='Account only active data parts.')
@pass_context
def list_partitions_command(ctx, **kwargs):
    """List partitions."""
    print(get_partitions(ctx, format='PrettyCompact', **kwargs))


@partition_group.command(name='attach')
@option('-n', '--dry-run', is_flag=True)
@option('-a', '--all', is_flag=True)
@option('--database')
@option('-t', '--table')
@option('--partition', 'partition_id')
@pass_context
def attach_partitions_command(ctx, dry_run, all, database, table, partition_id):
    """Attach one or several partitions."""
    if not any((all, database, table, partition_id)):
        ctx.fail('At least one of --all, --database, --table, --partition options must be specified.')

    partitions = get_partitions(ctx, database, table, partition_id=partition_id, detached=True, format='JSON')['data']
    for p in partitions:
        attach_partition(ctx, p['database'], p['table'], p['partition_id'], dry_run=dry_run)


@partition_group.command(name='detach')
@option('-n', '--dry-run', is_flag=True)
@option('-a', '--all', is_flag=True)
@option('--database')
@option('-t', '--table')
@option('--partition', 'partition_id')
@pass_context
def detach_partitions_command(ctx, dry_run, all, database, table, partition_id):
    """Detach one or several partitions."""
    if not any((all, database, table, partition_id)):
        ctx.fail('At least one of --all, --database, --table, --partition options must be specified.')

    partitions = get_partitions(ctx, database, table, partition_id=partition_id, format='JSON')['data']
    for p in partitions:
        detach_partition(ctx, p['database'], p['table'], p['partition_id'], dry_run=dry_run)


@partition_group.command(name='reattach')
@option('-n', '--dry-run', is_flag=True)
@option('-a', '--all', is_flag=True)
@option('--database')
@option('-t', '--table')
@option('--partition', 'partition_id')
@pass_context
def reattach_partitions_command(ctx, dry_run, all, database, table, partition_id):
    """Perform sequential attach and detach of one or several partitions."""
    if not any((all, database, table, partition_id)):
        ctx.fail('At least one of --all, --database, --table, --partition options must be specified.')

    partitions = get_partitions(ctx, database, table, partition_id=partition_id, format='JSON')['data']
    for p in partitions:
        detach_partition(ctx, p['database'], p['table'], p['partition_id'], dry_run=dry_run)
        attach_partition(ctx, p['database'], p['table'], p['partition_id'], dry_run=dry_run)


@partition_group.command(name='delete')
@option('-n', '--dry-run', is_flag=True)
@option('--database')
@option('-t', '--table')
@option('--partition', 'partition_id')
@option('--min-partition')
@option('--max-partition')
@option('--min-date')
@option('--max-date')
@pass_context
def delete_partitions_command(
    ctx, dry_run, database, table, partition_id, min_partition, max_partition, min_date, max_date
):
    """Delete one or several partitions."""
    if not any((database, table, partition_id, min_partition, max_partition, min_date, max_date)):
        ctx.fail(
            'At least one of --database, --table, --partition, --min-partition, --max-partition,'
            ' --min-date and --max-date options must be specified.'
        )

    partitions = get_partitions(
        ctx,
        database,
        table,
        partition_id=partition_id,
        min_partition=min_partition,
        max_partition=max_partition,
        min_date=min_date,
        max_date=max_date,
        format='JSON',
    )['data']
    for p in partitions:
        drop_partition(ctx, p['database'], p['table'], p['partition_id'], dry_run=dry_run)


@partition_group.command(name='optimize')
@option('-n', '--dry-run', is_flag=True)
@option('--database')
@option('-t', '--table')
@option('--partition', 'partition_id')
@option('--min-partition')
@option('--max-partition')
@option('--min-date')
@option('--max-date')
@pass_context
def optimize_partitions_command(
    ctx, dry_run, database, table, partition_id, min_partition, max_partition, min_date, max_date
):
    """Optimize partitions."""
    if not any((database, table, partition_id, min_partition, max_partition, min_date, max_date)):
        ctx.fail(
            'At least one of --database, --table, --partition, --min-partition, --max-partition,'
            ' --min-date and --max-date options must be specified.'
        )

    for p in get_partitions(
        ctx,
        database,
        table,
        partition_id=partition_id,
        min_partition=min_partition,
        max_partition=max_partition,
        min_date=min_date,
        max_date=max_date,
        format='JSON',
    )['data']:
        optimize_partition(ctx, p['database'], p['table'], p['partition_id'], dry_run=dry_run)


def get_partitions(
    ctx,
    database,
    table,
    partition_id=None,
    min_partition=None,
    max_partition=None,
    min_date=None,
    max_date=None,
    active_parts=None,
    detached=None,
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
            {% if min_partition %}
              AND partition_id >= '{{ min_partition }}'
            {% endif %}
            {% if max_partition %}
              AND partition_id <= '{{ max_partition }}'
            {% endif %}
            ORDER BY database, table, partition_id
            """
    else:
        query = """
            SELECT
                database,
                table,
                partition_id,
                count() "parts",
                min(min_date) "min_date",
                max(max_date) "max_date",
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
            {% if partition_id %}
              AND partition_id {{ format_str_match(partition_id) }}
            {% endif %}
            {% if min_partition %}
              AND partition_id >= '{{ min_partition }}'
            {% endif %}
            {% if max_partition %}
              AND partition_id <= '{{ max_partition }}'
            {% endif %}
            {% if min_date %}
              AND max_date >= '{{ min_date }}'
            {% endif %}
            {% if max_date %}
              AND min_date <= '{{ max_date }}'
            {% endif %}
            ORDER BY database, table, partition_id
            """
    return execute_query(
        ctx,
        query,
        database=database,
        table=table,
        partition_id=partition_id,
        min_partition=min_partition,
        max_partition=max_partition,
        min_date=min_date,
        max_date=max_date,
        active_parts=active_parts,
        format=format,
    )


def attach_partition(ctx, database, table, partition_id, dry_run=False):
    """
    Attach the specified table partition.
    """
    query = f'ALTER TABLE `{database}`.`{table}` ATTACH PARTITION ID \'{partition_id}\''
    execute_query(ctx, query, echo=True, dry_run=dry_run)


def detach_partition(ctx, database, table, partition_id, dry_run=False):
    """
    Detach the specified table partition.
    """
    query = f'ALTER TABLE `{database}`.`{table}` DETACH PARTITION ID \'{partition_id}\''
    execute_query(ctx, query, echo=True, dry_run=dry_run)


def drop_partition(ctx, database, table, partition_id, dry_run=False):
    """
    Drop the specified table partition.
    """
    query = f'ALTER TABLE `{database}`.`{table}` DROP PARTITION ID \'{partition_id}\''
    execute_query(ctx, query, echo=True, dry_run=dry_run)


def optimize_partition(ctx, database, table, partition_id, dry_run=False):
    """
    Optimize the specified table partition.
    """
    query = f'OPTIMIZE TABLE `{database}`.`{table}` PARTITION ID \'{partition_id}\''
    execute_query(ctx, query, echo=True, dry_run=dry_run)


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
