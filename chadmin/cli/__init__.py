from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseConfig
from kazoo.client import KazooClient


def execute_query(ctx, query, echo=False, dry_run=False, format='default', **kwargs):
    if format == 'default':
        format = 'PrettyCompact'

    return ctx.obj['chcli'].query(query, query_args=kwargs, format=format, echo=echo, dry_run=dry_run)


def zk_client(ctx, host, port, zkcli_identity):
    """
    Create and return KazooClient.
    """
    zk_config = get_config(ctx).zookeeper
    connect_str = ','.join(f'{host if host else node["host"]}:{port if port else node["port"]}' for node in zk_config['node'])
    if 'root' in zk_config:
        connect_str += zk_config['root']

    if zkcli_identity is None:
        if 'identity' in zk_config:
            zkcli_identity = zk_config['identity']

    auth_data = None
    if zkcli_identity is not None:
        auth_data=[("digest", zkcli_identity)]

    return KazooClient(connect_str, auth_data=auth_data)


def get_config(ctx):
    if 'config' not in ctx.obj:
        ctx.obj['config'] = ClickhouseConfig.load()

    return ctx.obj['config']


def get_macros(ctx):
    return get_config(ctx).macros


def get_cluster_name(ctx):
    return get_config(ctx).cluster_name
