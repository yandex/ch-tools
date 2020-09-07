import click
import time

from cloud.mdb.clickhouse.tools.monrun_checks.clickhouse_client import ClickhouseClient, ClickhousePort
from cloud.mdb.clickhouse.tools.monrun_checks.result import Result


@click.command('ping')
@click.option('-n', '--number', 'number', type=int, default=10, help='The max number of retries.')
def ping_command(number):
    """
    Ping all available clickhouse ports.
    """
    ch_client = ClickhouseClient()

    error = 'Unexpected error'
    for n in range(number):
        try:
            if ch_client.execute('SELECT 1', port=ClickhousePort.tcp)[0][0] != 1:
                error = 'Can\'t check tcp_port'
                continue

            if ch_client.check_port(ClickhousePort.tcps):
                if ch_client.execute('SELECT 1', port=ClickhousePort.tcps)[0][0] != 1:
                    error = 'Can\'t check tcp_port_secure'
                    continue

            if ch_client.check_port(ClickhousePort.http):
                if ch_client.ping(ClickhousePort.http) != 'Ok.':
                    error = 'Can\'t ping http_port'
                    continue
                if ch_client.execute('SELECT 1', port=ClickhousePort.http)[0][0] != 1:
                    error = 'Can\'t check http_port'
                    continue

            if ch_client.check_port(ClickhousePort.https):
                if ch_client.ping(ClickhousePort.https) != 'Ok.':
                    error = 'Can\'t ping https_port'
                    continue
                if ch_client.execute('SELECT 1', port=ClickhousePort.https)[0][0] != 1:
                    error = 'Can\'t check https_port'
                    continue

            return Result(0, 'OK')

        except Exception as e:
            error = repr(e)

        time.sleep(1)

    return Result(2, f'ClickHouse is dead ({error})')
