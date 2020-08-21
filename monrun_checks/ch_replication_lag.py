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
    lag = get_replication_lag()
    if lag < warn:
        return Result(code=0, message='OK')

    msg = '{0} seconds'.format(lag)

    versions_count = ClickhouseInfo.get_versions_count()
    if versions_count > 1:
        msg += ', ClickHouse versions on replicas mismatch'

    if lag < crit or versions_count > 1:
        return Result(code=1, message=msg)

    return Result(code=2, message=msg)


def get_replication_lag():
    """
    Get max absolute_delay from system.replicas.
    """
    tables = get_tables_with_replication_delay()
    tables = filter_out_single_replica_tables(tables)
    return get_max_replication_delay(tables)


def get_tables_with_replication_delay():
    """
    Get tables with absolute_delay > 0.
    """
    query = 'SELECT database, table, zookeeper_path FROM system.replicas WHERE absolute_delay > 0'
    return ClickhouseClient().execute(query, False)


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


def get_max_replication_delay(tables):
    """
    Get max absolute_delay for the specified tables.
    """
    if not tables:
        return 0

    query = '''
        SELECT max(absolute_delay)
        FROM system.replicas
        WHERE (database, table) IN ({tables})
        '''.format(
        tables=','.join("('{0}', '{1}')".format(t['database'], t['table']) for t in tables))
    return int(ClickhouseClient().execute(query)[0][0])
