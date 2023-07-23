import logging
import time

import click

from ch_tools.common.result import Result
from ch_tools.monrun_checks.clickhouse_client import ClickhouseClient, ClickhousePort


@click.command("ping")
@click.option(
    "-n", "--number", "number", type=int, default=10, help="The max number of retries."
)
@click.option(
    "-c", "--critical", "crit", type=int, default=5, help="Critical threshold."
)
@click.option("-w", "--warning", "warn", type=int, default=2, help="Warning threshold.")
def ping_command(number, crit, warn):
    """
    Ping all available ClickHouse ports.
    """
    # pylint: disable=too-many-branches

    ch_client = ClickhouseClient()

    fails = {
        ClickhousePort.TCP: 0,
        ClickhousePort.TCP_SECURE: 0,
        ClickhousePort.HTTP: 0,
        ClickhousePort.HTTPS: 0,
    }

    has_fails = False

    for _ in range(number):
        try:
            if ch_client.execute("SELECT 1", port=ClickhousePort.TCP)[0][0] != 1:
                fails[ClickhousePort.TCP] += 1
                has_fails = True
        except Exception as e:
            logging.debug("Error on tcp port: %s", repr(e))
            fails[ClickhousePort.TCP] += 1
            has_fails = True

        try:
            if ch_client.check_port(ClickhousePort.TCP_SECURE):
                if (
                    ch_client.execute("SELECT 1", port=ClickhousePort.TCP_SECURE)[0][0]
                    != 1
                ):
                    fails[ClickhousePort.TCP_SECURE] += 1
                    has_fails = True
        except Exception as e:
            logging.debug("Error on tcps port: %s", repr(e))
            fails[ClickhousePort.TCP_SECURE] += 1
            has_fails = True

        try:
            if ch_client.check_port(ClickhousePort.HTTP):
                if (
                    ch_client.ping(ClickhousePort.HTTP) != "Ok."
                    or ch_client.execute("SELECT 1", port=ClickhousePort.HTTP)[0][0]
                    != 1
                ):
                    fails[ClickhousePort.HTTP] += 1
                    has_fails = True
        except Exception as e:
            logging.debug("Error on http port: %s", repr(e))
            fails[ClickhousePort.HTTP] += 1
            has_fails = True

        try:
            if ch_client.check_port(ClickhousePort.HTTPS):
                if (
                    ch_client.ping(ClickhousePort.HTTPS) != "Ok."
                    or ch_client.execute("SELECT 1", port=ClickhousePort.HTTPS)[0][0]
                    != 1
                ):
                    fails[ClickhousePort.HTTPS] += 1
                    has_fails = True
        except Exception as e:
            logging.debug("Error on https port: %s", repr(e))
            fails[ClickhousePort.HTTPS] += 1
            has_fails = True

        if not has_fails:  # when all ports are ok on first time
            return Result(0, "OK")

        time.sleep(1)

    state = 0
    errors = []
    for port, cnt in fails.items():
        if cnt > 0:
            port_num = ch_client.get_port(port)
            errors.append(f"port {port_num}: {cnt}/{number} fails")
        if cnt >= crit:
            state = 2
        elif cnt >= warn and state < 1:
            state = 1

    error = ", ".join(errors)

    if state == 2:
        return Result(2, f"ClickHouse is dead ({error})")

    if state == 1:
        return Result(1, f"ClickHouse is sick ({error})")

    return Result(0, f"OK ({error})")
