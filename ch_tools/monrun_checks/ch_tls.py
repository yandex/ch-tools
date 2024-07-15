from typing import List, Optional

import click

from ch_tools.common.clickhouse.client.clickhouse_client import (
    ClickhousePort,
    clickhouse_client,
)
from ch_tools.common.result import Result
from ch_tools.common.tls import check_cert_on_ports

CERTIFICATE_PATH = "/etc/clickhouse-server/ssl/server.crt"


@click.command("tls")
@click.option("-c", "--critical", "crit", type=int, help="Critical threshold.")
@click.option("-w", "--warning", "warn", type=int, help="Warning threshold.")
@click.option(
    "-p",
    "--ports",
    "ports",
    type=str,
    default=None,
    help="Comma separated list of ports. By default read from ClickHouse config",
)
@click.option("--chain", "chain", is_flag=True, help="Verify certificate chain.")
@click.pass_context
def tls_command(
    ctx: click.Context,
    crit: int,
    warn: int,
    ports: Optional[str],
    chain: bool,
) -> Result:
    """
    Check TLS certificate for expiration and that actual cert from fs used.
    """
    # pylint: disable=too-many-return-statements

    return check_cert_on_ports(
        get_ports(ctx, ports), crit, warn, chain, CERTIFICATE_PATH
    )


def get_ports(ctx: click.Context, ports: Optional[str]) -> List[str]:
    if ports:
        return ports.split(",")
    client = clickhouse_client(ctx)
    result = []
    if client.check_port(ClickhousePort.HTTPS):
        result.append(client.get_port(ClickhousePort.HTTPS))
    if client.check_port(ClickhousePort.TCP_SECURE):
        result.append(client.get_port(ClickhousePort.TCP_SECURE))
    return result
