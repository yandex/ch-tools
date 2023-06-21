import click

from kazoo.client import KazooClient, KazooException
from kazoo.handlers.threading import KazooTimeoutError

from chtools.common.clickhouse.config import ClickhouseKeeperConfig
from chtools.common.result import Result


@click.command('keeper')
@click.option('-r', '--retries', 'retries', type=int, default=3, help='Connection retries')
@click.option('-t', '--timeout', 'timeout', type=int, default=10, help='Connection timeout (s)')
def keeper_command(retries, timeout) -> Result:
    """
    Checks ClickHouse Keeper is alive.
    """
    keeper_port, use_ssl = ClickhouseKeeperConfig.load().port_pair
    if not keeper_port:
        return Result(0, 'disabled')

    client = KazooClient(
        f'127.0.0.1:{keeper_port}',
        connection_retry=retries,
        command_retry=retries,
        timeout=timeout,
        use_ssl=use_ssl,
    )
    try:
        client.start()
        client.get('/')
        client.stop()
    except (KazooException, KazooTimeoutError) as e:
        return Result(2, repr(e))

    return Result(0, "OK")
