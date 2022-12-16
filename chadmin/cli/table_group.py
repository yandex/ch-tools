from click import argument, Choice, group, option, pass_context
from cloud.mdb.cli.common.formatting import print_response
from cloud.mdb.clickhouse.tools.chadmin.cli import get_cluster_name
from cloud.mdb.clickhouse.tools.chadmin.internal.table import attach_table, detach_table, get_table, list_tables

from cloud.mdb.clickhouse.tools.chadmin.internal.utils import execute_query


@group('table')
def table_group():
    """Commands to manage tables."""
    pass


@table_group.command('get')
@argument('database')
@argument('table')
@option('--active', '--active-parts', 'active_parts', is_flag=True, help='Account only active data parts.')
@pass_context
def get_command(ctx, database, table, active_parts):
    """
    Get table.
    """
    table = get_table(ctx, database, table, active_parts=active_parts)
    print_response(ctx, table, default_format='yaml')


@table_group.command('list')
@option('--database', help='Filter tables to output by the specified database.')
@option('-t', '--table', help='Output only the specified table.')
@option('--exclude-table', help='Do not output the specified table.')
@option('--engine', help='Filter tables to output by the specified engine.')
@option('--active', '--active-parts', 'active_parts', is_flag=True, help='Account only active data parts.')
@option('-v', '--verbose', is_flag=True, help='Verbose mode.')
@option('--order-by', type=Choice(['size', 'parts', 'rows']))
@option('-l', '--limit', type=int, default=1000, help='Limit the max number of objects in the output.')
@pass_context
def list_command(ctx, database, table, exclude_table, engine, active_parts, verbose, order_by, limit):
    """
    List tables.
    """
    tables = list_tables(
        ctx,
        database=database,
        table=table,
        exclude_table=exclude_table,
        engine=engine,
        active_parts=active_parts,
        verbose=verbose,
        order_by=order_by,
        limit=limit,
    )
    print_response(ctx, tables, default_format='table')


@table_group.command('columns')
@argument('database')
@argument('table')
@pass_context
def columns_command(ctx, database, table):
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
@option('--database')
@option('-t', '--table')
@option('--exclude-table')
@option('-a', '--all', is_flag=True, help='Delete all tables.')
@option('--cluster')
@option(
    '-n', '--dry-run', is_flag=True, default=False, help='Enable dry run mode and do not perform any modifying actions.'
)
def delete_command(ctx, dry_run, all, database, table, exclude_table, cluster):
    """
    Delete one or several tables.
    """
    if not any((all, database, table)):
        ctx.fail('At least one of --all, --database, --table options must be specified.')

    for t in list_tables(ctx, database=database, table=table, exclude_table=exclude_table):
        query = """
            DROP TABLE `{{ database }}`.`{{ table }}`
            {% if cluster %}
            ON CLUSTER '{{ cluster }}'
            {% endif %}
            NO DELAY
            """
        execute_query(ctx, query, database=t['database'], table=t['table'], cluster=cluster, echo=True, dry_run=dry_run)


@table_group.command('recreate')
@pass_context
@option('--database')
@option('-t', '--table')
@option('--exclude-table')
@option('-a', '--all', is_flag=True, help='Recreate all tables.')
@option(
    '-n', '--dry-run', is_flag=True, default=False, help='Enable dry run mode and do not perform any modifying actions.'
)
def recreate_command(ctx, dry_run, all, database, table, exclude_table):
    """
    Recreate one or several tables.
    """
    if not any((all, database, table)):
        ctx.fail('At least one of --all, --database, --table options must be specified.')

    for t in list_tables(ctx, database=database, table=table, exclude_table=exclude_table, verbose=True):
        drop_query = """DROP TABLE `{{ database }}`.`{{ table }}` NO DELAY"""
        execute_query(
            ctx, drop_query, database=t['database'], table=t['table'], echo=True, format=None, dry_run=dry_run
        )
        execute_query(ctx, t['create_table_query'], echo=True, format=None, dry_run=dry_run)


@table_group.command('detach')
@pass_context
@option('--database')
@option('-t', '--table')
@option('--engine', help='Filter tables to detach by the specified engine.')
@option('--exclude-table')
@option('-a', '--all', is_flag=True, help='Detach all tables.')
@option(
    '--cluster', '--on-cluster', 'on_cluster', is_flag=True, help='Perform detach queries with ON CLUSTER modificator.'
)
@option(
    '-n', '--dry-run', is_flag=True, default=False, help='Enable dry run mode and do not perform any modifying actions.'
)
def detach_command(ctx, dry_run, all, database, table, engine, exclude_table, on_cluster):
    """
    Detach one or several tables.
    """
    if not any((all, database, table)):
        ctx.fail('At least one of --all, --database, --table options must be specified.')

    cluster = get_cluster_name(ctx) if on_cluster else None

    for t in list_tables(ctx, database=database, table=table, engine=engine, exclude_table=exclude_table):
        detach_table(ctx, database=t['database'], table=t['table'], cluster=cluster, echo=True, dry_run=dry_run)


@table_group.command('reattach')
@pass_context
@option('--database')
@option('-t', '--table')
@option('--engine', help='Filter tables to detach by the specified engine.')
@option('--exclude-table')
@option('-a', '--all', is_flag=True, help='Reattach all tables.')
@option(
    '--cluster',
    '--on-cluster',
    'on_cluster',
    is_flag=True,
    help='Perform attach and detach queries with ON CLUSTER modificator.',
)
@option(
    '-n', '--dry-run', is_flag=True, default=False, help='Enable dry run mode and do not perform any modifying actions.'
)
def reattach_command(ctx, dry_run, all, database, table, engine, exclude_table, on_cluster):
    """
    Reattach one or several tables.
    """
    if not any((all, database, table)):
        ctx.fail('At least one of --all, --database, --table options must be specified.')

    cluster = get_cluster_name(ctx) if on_cluster else None

    for t in list_tables(ctx, database=database, table=table, engine=engine, exclude_table=exclude_table):
        detach_table(ctx, database=t['database'], table=t['table'], cluster=cluster, echo=True, dry_run=dry_run)
        attach_table(ctx, database=t['database'], table=t['table'], cluster=cluster, echo=True, dry_run=dry_run)


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

    for t in list_tables(ctx, database=database):
        stats = execute_query(ctx, query, table=t['table'], format='JSON')['data'][0]
        print('{0}: {1}'.format(t['table'], stats['count']))
