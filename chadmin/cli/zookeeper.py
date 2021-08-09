import os
from pprint import pprint

from click import argument, group, pass_context, option

from . import get_macros, zk_client

zk_client_args = {}

@group('zookeeper')
@option('--port', help='ZooKeeper port.', type=int)
@option('--host', help='ZooKeeper host.', type=str)
@option('--zkcli_identity', help='Identity for zookeeper cli shell. In a format login:password. '
                                 'Example: clickhouse:X7ui1dXIXXXXXXXXXXXXXXXXXXXXXXXX', type=str)
def zookeeper_group(host, port, zkcli_identity):
    """ZooKeeper management commands.

    ZooKeeper command runs client which connects to Zookeeper node.
    By default, it parses config on ClickHouse node in default config location and figures out host and port.
    You can override some of these parameters, but others will be figured out from configs.
    """

    global zk_client_args
    zk_client_args['port'] = port
    zk_client_args['host'] = host
    zk_client_args['zkcli_identity'] = zkcli_identity


def run_client(ctx, command=None, *args, host=None, port=None, zkcli_identity=None):
    zk = zk_client(ctx, host=host, port=port, zkcli_identity=zkcli_identity)
    try:
        zk.start()
        command(zk, *args)
    finally:
        zk.stop()


@zookeeper_group.command(name='get')
@argument('path')
@pass_context
def get_command(ctx, path):
    """Get ZooKeeper node.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    def command(zk, path):
        path = format_path(ctx, path)
        result = zk.get(path)
        pprint(result)

    run_client(ctx, command, path, **zk_client_args)


@zookeeper_group.command(name='list')
@argument('path')
@pass_context
def list_command(ctx, path):
    """List ZooKeeper nodes.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    def command(zk, path):
        path = format_path(ctx, path)
        result = zk.get_children(path)
        print('\n'.join(os.path.join(path, node) for node in sorted(result)))

    run_client(ctx, command, path, **zk_client_args)


@zookeeper_group.command(name='stat')
@argument('path')
@pass_context
def stat_command(ctx, path):
    """Show statistics for ZooKeeper node.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """

    def command(zk, path):
        path = format_path(ctx, path)
        result = zk.get_acls(path)[1]
        print(result)

    run_client(ctx, command, path, **zk_client_args)


@zookeeper_group.command(name='delete')
@argument('path')
@pass_context
def delete_command(ctx, path):
    """Delete ZooKeeper node.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    def command(zk, path):
        path = format_path(ctx, path)
        zk.delete(path, recursive=True)

    run_client(ctx, command, path, **zk_client_args)


@zookeeper_group.command(name='create')
@argument('paths', nargs=-1)
@pass_context
def create_command(ctx, paths):
    """Create ZooKeeper nodes, accepts one or more paths.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    def command(zk, paths):
        for path in paths:
            path = format_path(ctx, path)
            zk.create(path)

    run_client(ctx, command, paths, **zk_client_args)


def format_path(ctx, path):
    return path.format_map(get_macros(ctx))
