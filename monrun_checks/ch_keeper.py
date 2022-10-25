import click

from kazoo.client import KazooClient, KazooException
from kazoo.handlers.threading import KazooTimeoutError

from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseKeeperConfig
from cloud.mdb.clickhouse.tools.monrun_checks.result import Result


@click.command('keeper')
@click.option('-r', '--retries', 'retries', type=int, default=3, help='Connection retries')
@click.option('-t', '--timeout', 'timeout', type=int, default=10, help='Connection timeout (s)')
def keeper_command(retries, timeout) -> Result:
    """
    Checks ClickHouse Keeper is alive.
    """
    zk_port = ClickhouseKeeperConfig.load().port
    if not zk_port:
        return Result(0, 'disabled')

    client = KazooClient(
        f'127.0.0.1:{zk_port}',
        connection_retry=retries,
        command_retry=retries,
        timeout=timeout,
    )
    try:
        client.start()
        client.get('/')
        client.stop()
    except (KazooException, KazooTimeoutError) as e:
        return Result(2, repr(e))

    return Result(0, "OK")
