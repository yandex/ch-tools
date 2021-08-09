from click import argument, group, option, pass_context

from cloud.mdb.clickhouse.tools.chadmin.cli import execute_query


@group('table')
def table_group():
    """Table management commands."""
    pass


@table_group.command('get')
@argument('database')
@argument('table')
@option('--active-parts', is_flag=True, help='Account only active data parts.')
@pass_context
def get_table_command(ctx, database, table, active_parts):
    print(get_tables(ctx, database=database, table=table, active_parts=active_parts, verbose=True, format='Vertical'))


@table_group.command('list')
@option('--database')
@option('-t', '--table')
@option('--exclude-table')
@option('--engine')
@option('--active-parts', is_flag=True, help='Account only active data parts.')
@option('-v', '--verbose', is_flag=True)
@pass_context
def list_tables_command(ctx, database, table, exclude_table, engine, active_parts, verbose):
    format = 'Vertical' if verbose else 'PrettyCompact'
    print(
        get_tables(ctx,
                   database=database,
                   table=table,
                   exclude_table=exclude_table,
                   engine=engine,
                   active_parts=active_parts,
                   verbose=verbose,
                   format=format))


@table_group.command('columns')
@argument('database')
@argument('table')
@pass_context
def get_table_columns_command(ctx, database, table):
    query = """
        SELECT
            name,
            type,
            default_kind,
            default_expression,
            formatReadableSize(data_compressed_bytes) "disk_size",
            formatReadableSize(data_uncompressed_bytes) "uncompressed_size",
            marks_bytes
        FROM system.columns
        WHERE database = '{{ database }}'
          AND table = '{{ table }}'
        """
    print(execute_query(ctx, query, database=database, table=table))


@table_group.command('delete')
@pass_context
@option('-n', '--dry-run', is_flag=True)
@option('-a', '--all', is_flag=True)
@option('--database')
@option('-t', '--table')
@option('--exclude-table')
@option('--cluster')
def delete_tables_command(ctx, dry_run, all, database, table, exclude_table, cluster):
    if not any((all, database, table)):
        ctx.fail('At least one of --all, --database and --table options must be specified.')

    for t in get_tables(ctx, database=database, table=table, exclude_table=exclude_table, format='JSON')['data']:
        query = """
            DROP TABLE `{{ database }}`.`{{ table }}`
            {% if cluster %}
            ON CLUSTER '{{ cluster }}'
            {% endif %}
            """
        execute_query(ctx,
                      query,
                      database=t['database'],
                      table=t['table'],
                      cluster=cluster,
                      echo=True,
                      dry_run=dry_run)


@table_group.command('get-statistics')
@option('--database')
@pass_context
def get_statistics_command(ctx, database):
    query = """
        SELECT count() count
        FROM system.query_log
        WHERE type != 1
        AND query LIKE '%{{ table }}%'
    """

    for t in get_tables(ctx, database=database, format='JSON')['data']:
        stats = execute_query(ctx, query, table=t['table'], format='JSON')['data'][0]
        print('{0}: {1}'.format(t['table'], stats['count']))


def get_tables(ctx,
               database,
               table=None,
               exclude_table=None,
               engine=None,
               active_parts=None,
               verbose=None,
               format=None):
    query = """
        SELECT
            database,
            table,
            disk_size,
            partitions,
            parts,
            rows,
            metadata_mtime,
        {% if verbose %}
            engine,
            create_table_query
        {% else %}
            engine
        {% endif %}
        FROM (
            SELECT
                database,
                name "table",
                metadata_modification_time "metadata_mtime",
                engine,
                create_table_query
             FROM system.tables
        ) tables
        ALL LEFT JOIN (
             SELECT
                 database,
                 table,
                 uniq(partition) "partitions",
                 count() "parts",
                 sum(rows) "rows",
                 formatReadableSize(sum(bytes_on_disk)) "disk_size"
             FROM system.parts
        {% if active_parts %}
             WHERE active
        {% endif %}
             GROUP BY database, table
        ) parts USING database, table
        {% if database %}
        WHERE database {{ format_str_match(database) }}
        {% else %}
        WHERE database != 'system'
        {% endif %}
        {% if table %}
          AND table {{ format_str_match(table) }}
        {% endif %}
        {% if exclude_table %}
          AND table NOT {{ format_str_match(exclude_table) }}
        {% endif %}
        {% if engine %}
          AND engine {{ format_str_match(engine) }}
        {% endif %}
        ORDER BY database, table
        """
    return execute_query(ctx,
                         query,
                         database=database,
                         table=table,
                         exclude_table=exclude_table,
                         engine=engine,
                         active_parts=active_parts,
                         verbose=verbose,
                         format=format)
