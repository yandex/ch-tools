import time

import click

from ch_tools.common import logging
from ch_tools.common.clickhouse.client.clickhouse_client import (
    ClickhousePort,
    clickhouse_client,
)
from ch_tools.common.result import CRIT, OK, WARNING, Result


@click.command("ping")
@click.option(
    "-n", "--number", "number", type=int, default=10, help="The max number of retries."
)
@click.option(
    "-c", "--critical", "crit", type=int, default=5, help="Critical threshold."
)
@click.option("-w", "--warning", "warn", type=int, default=2, help="Warning threshold.")
@click.pass_context
def ping_command(ctx: click.Context, number: int, crit: int, warn: int) -> Result:
    """
    Ping all available ClickHouse ports.
    """
    # pylint: disable=too-many-branches

    ch_client = clickhouse_client(ctx)

    fails = {
        ClickhousePort.TCP: 0,
        ClickhousePort.TCP_SECURE: 0,
        ClickhousePort.HTTP: 0,
        ClickhousePort.HTTPS: 0,
    }

    has_fails = False

    for _ in range(number):
        try:
            if (
                ch_client.query_json_data(query="SELECT 1", port=ClickhousePort.TCP)[0][
                    0
                ]
                != 1
            ):
                fails[ClickhousePort.TCP] += 1
                has_fails = True
        except Exception as e:
            logging.debug("Error on tcp port: {!r}", e)
            fails[ClickhousePort.TCP] += 1
            has_fails = True

        try:
            if ch_client.check_port(ClickhousePort.TCP_SECURE):
                if (
                    ch_client.query_json_data(
                        query="SELECT 1", port=ClickhousePort.TCP_SECURE
                    )[0][0]
                    != 1
                ):
                    fails[ClickhousePort.TCP_SECURE] += 1
                    has_fails = True
        except Exception as e:
            logging.debug("Error on tcps port: {!r}", e)
            fails[ClickhousePort.TCP_SECURE] += 1
            has_fails = True

        try:
            if ch_client.check_port(ClickhousePort.HTTP):
                if (
                    ch_client.ping(ClickhousePort.HTTP) != "Ok."
                    or ch_client.query_json_data(
                        query="SELECT 1", port=ClickhousePort.HTTP
                    )[0][0]
                    != 1
                ):
                    fails[ClickhousePort.HTTP] += 1
                    has_fails = True
        except Exception as e:
            logging.debug("Error on http port: {!r}", e)
            fails[ClickhousePort.HTTP] += 1
            has_fails = True

        try:
            if ch_client.check_port(ClickhousePort.HTTPS):
                if (
                    ch_client.ping(ClickhousePort.HTTPS) != "Ok."
                    or ch_client.query_json_data(
                        query="SELECT 1", port=ClickhousePort.HTTPS
                    )[0][0]
                    != 1
                ):
                    fails[ClickhousePort.HTTPS] += 1
                    has_fails = True
        except Exception as e:
            logging.debug("Error on https port: {!r}", e)
            fails[ClickhousePort.HTTPS] += 1
            has_fails = True

        if not has_fails:  # when all ports are ok on first time
            return Result(OK)

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
        return Result(CRIT, f"ClickHouse is dead ({error})")

    if state == 1:
        return Result(WARNING, f"ClickHouse is sick ({error})")

    return Result(OK, f"OK ({error})")
