import os
from contextlib import contextmanager
from pprint import pprint

from click import argument, group, option, pass_context
from kazoo.exceptions import NoNodeError

from cloud.mdb.clickhouse.tools.chadmin.cli import get_macros, zk_client
from cloud.mdb.cli.common.cli import print_response

zk_client_args = {}


@group('zookeeper')
@option('--port', help='ZooKeeper port.', type=int, default=2181)
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


@zookeeper_group.command(name='get')
@argument('path')
@pass_context
def get_command(ctx, path):
    """Get ZooKeeper node.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    with _zk_client(ctx) as zk:
        path = format_path(ctx, path)
        result = zk.get(path)
        pprint(result)


@zookeeper_group.command(name='list')
@argument('path')
@option('-v', '--verbose', is_flag=True)
@pass_context
def list_command(ctx, path, verbose):
    """List ZooKeeper nodes.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    def _stat_node(zk, node):
        descendants_count = 0
        queue = [node]
        while queue:
            item = queue.pop()
            try:
                children = zk.get_children(item)
                descendants_count += len(children)
                queue.extend(os.path.join(item, node) for node in children)
            except NoNodeError:
                # ZooKeeper nodes can be deleted during node tree traversal
                pass

        return {
            'path': node,
            'nodes': descendants_count,
        }

    with _zk_client(ctx) as zk:
        path = format_path(ctx, path)
        result = zk.get_children(path)
        nodes = [os.path.join(path, node) for node in sorted(result)]
        if verbose:
            print_response(ctx, [_stat_node(zk, node) for node in nodes], format='table')
        else:
            print('\n'.join(nodes))


@zookeeper_group.command(name='stat')
@argument('path')
@pass_context
def stat_command(ctx, path):
    """Show statistics for ZooKeeper node.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    with _zk_client(ctx) as zk:
        path = format_path(ctx, path)
        result = zk.get_acls(path)[1]
        print(result)


@zookeeper_group.command(name='delete')
@argument('path')
@pass_context
def delete_command(ctx, path):
    """Delete ZooKeeper node.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    with _zk_client(ctx) as zk:
        path = format_path(ctx, path)
        zk.delete(path, recursive=True)


@zookeeper_group.command(name='create')
@argument('paths', nargs=-1)
@pass_context
def create_command(ctx, paths):
    """Create ZooKeeper nodes, accepts one or more paths.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    with _zk_client(ctx) as zk:
        for path in paths:
            path = format_path(ctx, path)
            zk.create(path)


def format_path(ctx, path):
    return path.format_map(get_macros(ctx))


@contextmanager
def _zk_client(ctx):
    zk = zk_client(ctx, **zk_client_args)
    try:
        zk.start()
        yield zk
    finally:
        zk.stop()
