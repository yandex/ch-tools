from click.exceptions import ClickException
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException


from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseConfig
from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseKeeperConfig


def get_keeper_client(ctx) -> KazooClient:
    client = KazooClient(hosts=_get_keeper_hosts(ctx))
    try:
        client.start()
    except KazooException as e:
        raise ClickException(f'cannot connect to keeper: {repr(e)}')

    return client


def _get_keeper_hosts(ctx) -> str:
    ch_keeper_config: ClickhouseKeeperConfig = ctx.obj['ch_keeper_config']
    ch_keeper_port = ch_keeper_config.port
    if ch_keeper_port is not None:
        return f'127.0.0.1:{ch_keeper_port}'

    ch_config: ClickhouseConfig = ctx.obj['ch_config']

    return ','.join(f'{node["host"]}:{node["port"]}' for node in ch_config.zookeeper.nodes)
