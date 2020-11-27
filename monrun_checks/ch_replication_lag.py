import click

from cloud.mdb.clickhouse.tools.monrun_checks.clickhouse_client import ClickhouseClient
from cloud.mdb.clickhouse.tools.monrun_checks.clickhouse_info import ClickhouseInfo
from cloud.mdb.clickhouse.tools.monrun_checks.result import Result


@click.command('replication-lag')
@click.option('-c', '--critical', 'crit', type=int, default=600, help='Critical threshold.')
@click.option('-w', '--warning', 'warn', type=int, default=300, help='Warning threshold.')
def replication_lag_command(crit, warn):
    """
    Check for replication lag between replicas.
    """
    lag, lag_with_errors = get_replication_lag()
    if lag < warn:
        return Result(code=0, message='OK')

    msg = 'Max {0} seconds, with errors {1} seconds'.format(lag, lag_with_errors)

    versions_count = ClickhouseInfo.get_versions_count()
    if versions_count > 1:
        msg += ', ClickHouse versions on replicas mismatch'

    if lag_with_errors < crit or versions_count > 1:
        return Result(code=1, message=msg)

    return Result(code=2, message=msg)


def get_replication_lag():
    """
    Get max absolute_delay from system.replicas.
    """

    tables = get_tables_with_replication_delay()
    chart = {}
    for t in tables:
        key = '{database}.{table}'.format(database=t['database'], table=t['table'])
        chart[key] = {}
        chart[key]['delay'] = int(t['absolute_delay'])
        chart[key]['multi_replicas'] = False
        chart[key]['errors'] = 0
    tables = filter_out_single_replica_tables(tables)
    for t in tables:
        key = '{database}.{table}'.format(database=t['database'], table=t['table'])
        chart[key]['multi_replicas'] = True
    tables = count_errors(tables)
    for t in tables:
        key = '{database}.{table}'.format(database=t['database'], table=t['table'])
        chart[key]['errors'] = int(t['errors'])

    lag = 0
    lag_with_errors = 0
    for t in chart:
        if chart[t]['multi_replicas']:
            delay = chart[t]['delay']
            if delay > lag:
                lag = delay
            if delay > lag_with_errors and chart[t]['errors'] > 0:
                lag_with_errors = delay

    return lag, lag_with_errors


def get_tables_with_replication_delay():
    """
    Get tables with absolute_delay > 0.
    """
    query = 'SELECT database, table, zookeeper_path, absolute_delay FROM system.replicas WHERE absolute_delay > 0'
    return ClickhouseClient().execute(query, compact=False)


def filter_out_single_replica_tables(tables):
    if not tables:
        return tables

    replicas = ClickhouseInfo.get_replicas()
    query = '''
        SELECT
            database,
            table,
            zookeeper_path
        FROM system.replicas
        WHERE (database, table) IN ({tables})
        AND total_replicas > 1
        AND zookeeper_path in (SELECT zookeeper_path
                                FROM remote('{replicas}', system.replicas)
                                GROUP BY zookeeper_path
                                HAVING count() > 1)
        '''.format(
        tables=','.join("('{0}', '{1}')".format(t['database'], t['table']) for t in tables),
        replicas=','.join(replicas))
    return ClickhouseClient().execute(query, False)


def count_errors(tables):
    '''
    Add count of replication errors
    '''
    if not tables:
        return tables

    query = '''
        SELECT
            database,
            table,
            count() as errors
        FROM system.replication_queue
        WHERE (database, table) IN ({tables})
        AND last_exception != ''
        AND postpone_reason = ''
        GROUP BY database,table
        '''.format(
        tables=','.join("('{0}', '{1}')".format(t['database'], t['table']) for t in tables))
    return ClickhouseClient().execute(query, False)
