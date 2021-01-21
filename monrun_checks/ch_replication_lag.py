import click
from tabulate import tabulate

from cloud.mdb.clickhouse.tools.monrun_checks.clickhouse_client import ClickhouseClient
from cloud.mdb.clickhouse.tools.monrun_checks.clickhouse_info import ClickhouseInfo
from cloud.mdb.clickhouse.tools.monrun_checks.result import Result


@click.command('replication-lag')
@click.option('-x', '--exec-critical', 'xcrit', type=int, default=3600, help='Critical threshold for one task execution.')
@click.option('-c', '--critical', 'crit', type=int, default=600, help='Critical threshold for lag with errors.')
@click.option('-w', '--warning', 'warn', type=int, default=300, help='Warning threshold.')
@click.option('-v', '--verbose', 'verbose', type=int, count=True, default=0, help='Show details about lag.')
def replication_lag_command(xcrit, crit, warn, verbose):
    """
    Check for replication lag between replicas.
    Should be: lag >= lag_with_errors, lag >= max_execution
    """
    lag, lag_with_errors, max_execution, chart = get_replication_lag()

    msg_verbose = ''
    msg_verbose_2 = '\n\n'

    if verbose >= 1:
        verbtab = []
        headers = ['Table', 'Lag', 'Errors', 'Max execution']
        for t in chart:
            if chart[t].get('multi_replicas', False):
                tabletab = [t, chart[t].get('delay', 0), chart[t].get('errors', 0), chart[t].get('max_execution', 0)]
                if verbose >= 2:
                    exceptions = ''
                    for exception in chart[t].get('exceptions', []):
                        if exception:
                            exceptions += '\t' + exception + '\n'
                    msg_verbose_2 = msg_verbose_2 + t + ':\n  Exceptions:\n' + exceptions
                    max_execution_part = chart[t].get('max_execution_part', '')
                    if max_execution_part:
                        msg_verbose_2 = msg_verbose_2 + '  Max execution part: ' + max_execution_part
                verbtab.append(tabletab)
        msg_verbose = tabulate(verbtab, headers=headers)
        if verbose >= 2:
            msg_verbose = msg_verbose + msg_verbose_2

    if lag < warn:
        return Result(code=0, message='OK', verbose=msg_verbose)

    msg = 'Max {0} seconds, with errors {1} seconds, max execution {2} seconds'.format(lag, lag_with_errors, max_execution)

    versions_count = ClickhouseInfo.get_versions_count()
    if versions_count > 1:
        msg += ', ClickHouse versions on replicas mismatch'

    if (lag_with_errors < crit and max_execution < xcrit) or versions_count > 1:
        return Result(code=1, message=msg, verbose=msg_verbose)

    return Result(code=2, message=msg, verbose=msg_verbose)


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
    tables = filter_out_single_replica_tables(tables)
    for t in tables:
        key = '{database}.{table}'.format(database=t['database'], table=t['table'])
        chart[key]['multi_replicas'] = True
    tables = count_errors(tables, -1)
    for t in tables:
        key = '{database}.{table}'.format(database=t['database'], table=t['table'])
        chart[key]['errors'] = int(t['errors'])
        chart[key]['max_execution'] = int(t['max_execution'])
        chart[key]['max_execution_part'] = t['max_execution_part']
        chart[key]['exceptions'] = t['exceptions']
        for exception in t['exceptions']:
            if is_userfault_exception(exception):
                chart[key]['userfault'] = True
                break

    lag = 0
    lag_with_errors = 0
    max_execution = 0
    for t in chart:
        if chart[t].get('multi_replicas', False):
            delay = chart[t].get('delay', 0)
            if delay > lag:
                lag = delay
            if delay > lag_with_errors and chart[t].get('errors', 0) > 0 and not chart[t].get('userfault', False):
                lag_with_errors = delay
            execution = chart[t].get('max_execution', 0)
            if execution > max_execution:
                max_execution = execution

    return lag, lag_with_errors, max_execution, chart


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


def count_errors(tables, exceptions_limit):
    """
    Add count of replication errors.
    """
    if not tables:
        return tables

    limit = '' if exceptions_limit < 0 else '({})'.format(exceptions_limit)

    query = '''
        SELECT
            database,
            table,
            countIf(last_exception != '' AND postpone_reason = '') as errors,
            max(IF(is_currently_executing, dateDiff('second', last_attempt_time, now()), 0)) as max_execution,
            groupUniqArray{limit}(last_exception) as exceptions,
            argMax(new_part_name, IF(is_currently_executing, dateDiff('second', last_attempt_time, now()), 0)) as max_execution_part
        FROM system.replication_queue
        WHERE (database, table) IN ({tables})
        GROUP BY database,table
        '''.format(
        tables=','.join("('{0}', '{1}')".format(t['database'], t['table']) for t in tables),
        limit=limit)
    return ClickhouseClient().execute(query, False)


def is_userfault_exception(exception):
    """
    Check if exception was caused by user.
    Current list:
      * DB::Exception: Cannot reserve 1.00 MiB, not enough space
      * DB::Exception: Incorrect data: Sign = -127 (must be 1 or -1)
    """

    if 'DB::Exception: Cannot reserve' in exception and 'not enough space' in exception:
        return True
    if 'DB::Exception: Incorrect data: Sign' in exception and '(must be 1 or -1)' in exception:
        return True

    return False
