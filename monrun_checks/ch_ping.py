import click
import logging
import time

from monrun_checks.clickhouse_client import ClickhouseClient, ClickhousePort
from common.result import Result


@click.command('ping')
@click.option('-n', '--number', 'number', type=int, default=10, help='The max number of retries.')
@click.option('-c', '--critical', 'crit', type=int, default=5, help='Critical threshold.')
@click.option('-w', '--warning', 'warn', type=int, default=2, help='Warning threshold.')
def ping_command(number, crit, warn):
    """
    Ping all available ClickHouse ports.
    """
    ch_client = ClickhouseClient()

    fails = {ClickhousePort.tcp: 0, ClickhousePort.tcps: 0, ClickhousePort.http: 0, ClickhousePort.https: 0}

    has_fails = False

    for n in range(number):
        try:
            if ch_client.execute('SELECT 1', port=ClickhousePort.tcp)[0][0] != 1:
                fails[ClickhousePort.tcp] += 1
                has_fails = True
        except Exception as e:
            logging.debug('Error on tcp port: %s', repr(e))
            fails[ClickhousePort.tcp] += 1
            has_fails = True

        try:
            if ch_client.check_port(ClickhousePort.tcps):
                if ch_client.execute('SELECT 1', port=ClickhousePort.tcps)[0][0] != 1:
                    fails[ClickhousePort.tcps] += 1
                    has_fails = True
        except Exception as e:
            logging.debug('Error on tcps port: %s', repr(e))
            fails[ClickhousePort.tcps] += 1
            has_fails = True

        try:
            if ch_client.check_port(ClickhousePort.http):
                if (
                    ch_client.ping(ClickhousePort.http) != 'Ok.'
                    or ch_client.execute('SELECT 1', port=ClickhousePort.http)[0][0] != 1
                ):
                    fails[ClickhousePort.http] += 1
                    has_fails = True
        except Exception as e:
            logging.debug('Error on http port: %s', repr(e))
            fails[ClickhousePort.http] += 1
            has_fails = True

        try:
            if ch_client.check_port(ClickhousePort.https):
                if (
                    ch_client.ping(ClickhousePort.https) != 'Ok.'
                    or ch_client.execute('SELECT 1', port=ClickhousePort.https)[0][0] != 1
                ):
                    fails[ClickhousePort.https] += 1
                    has_fails = True
        except Exception as e:
            logging.debug('Error on https port: %s', repr(e))
            fails[ClickhousePort.https] += 1
            has_fails = True

        if not has_fails:  # when all ports are ok on first time
            return Result(0, 'OK')

        time.sleep(1)

    state = 0
    errors = []
    for port, cnt in fails.items():
        if cnt > 0:
            port_num = ch_client.get_port(port)
            errors.append(f'port {port_num}: {cnt}/{number} fails')
        if cnt >= crit:
            state = 2
        elif cnt >= warn and state < 1:
            state = 1

    error = ', '.join(errors)

    if state == 2:
        return Result(2, f'ClickHouse is dead ({error})')
    elif state == 1:
        return Result(1, f'ClickHouse is sick ({error})')
    else:
        return Result(0, f'OK ({error})')
