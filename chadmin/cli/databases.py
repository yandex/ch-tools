from click import argument, group, option, pass_context

from cloud.mdb.clickhouse.tools.chadmin.cli import execute_query


@group('database')
def database_group():
    """Database management commands."""
    pass


@database_group.command('get')
@argument('database')
@option('--active-parts', is_flag=True, help='Account only active data parts.')
@pass_context
def get_database_command(ctx, database, active_parts):
    print(get_databases(ctx, database=database, active_parts=active_parts, format='Vertical'))


@database_group.command('list')
@option('--database')
@option('--exclude-database')
@option('--active-parts', is_flag=True, help='Account only active data parts.')
@pass_context
def list_databases_command(ctx, database, exclude_database, active_parts):
    print(get_databases(ctx, database=database, exclude_database=exclude_database, active_parts=active_parts))


@database_group.command('delete')
@pass_context
@option('-n', '--dry-run', is_flag=True)
@option('-a', '--all', is_flag=True)
@option('--database')
@option('--exclude-database')
@option('--cluster')
def delete_databases_command(ctx, dry_run, all, database, exclude_database, cluster):
    if not any((all, database, exclude_database)):
        ctx.fail('At least one of --all, --database and --exclude-database options must be specified.')

    for d in get_databases(ctx, database=database, exclude_database=exclude_database, format='JSON')['data']:
        query = """
            DROP DATABASE `{{ database }}`
            {% if cluster %}
            ON CLUSTER '{{ cluster }}'
            {% endif %}
            """
        execute_query(ctx, query, database=d['database'], cluster=cluster, echo=True, dry_run=dry_run)


def get_databases(ctx, database=None, exclude_database=None, active_parts=None, format=None):
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
        WHERE database != 'system'
        {% endif %}
        {% if exclude_database %}
          AND database != '{{ exclude_database }}'
        {% endif %}
        ORDER BY database
        """
    return execute_query(ctx,
                         query,
                         database=database,
                         exclude_database=exclude_database,
                         active_parts=active_parts,
                         format=format)
