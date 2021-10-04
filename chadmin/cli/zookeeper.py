from pprint import pprint

from click import argument, group, option, pass_context

from cloud.mdb.cli.common.cli import print_response
from cloud.mdb.clickhouse.tools.chadmin.internal.zookeeper import create_zk_nodes, delete_zk_node, get_zk_node, \
    get_zk_node_acls, list_zk_nodes


@group('zookeeper')
@option('--port', help='ZooKeeper port.', type=int, default=2181)
@option('--host', help='ZooKeeper host.', type=str)
@option('--zkcli_identity', help='Identity for zookeeper cli shell. In a format login:password. '
                                 'Example: clickhouse:X7ui1dXIXXXXXXXXXXXXXXXXXXXXXXXX', type=str)
@pass_context
def zookeeper_group(ctx, host, port, zkcli_identity):
    """ZooKeeper management commands.

    ZooKeeper command runs client which connects to Zookeeper node.
    By default, it parses config on ClickHouse node in default config location and figures out host and port.
    You can override some of these parameters, but others will be figured out from configs.
    """

    ctx.obj['zk_client_args'] = {
        'port': port,
        'host': host,
        'zkcli_identity': zkcli_identity,
    }


@zookeeper_group.command(name='get')
@argument('path')
@pass_context
def get_command(ctx, path):
    """Get ZooKeeper node.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    pprint(get_zk_node(ctx, path))


@zookeeper_group.command(name='list')
@argument('path')
@option('-v', '--verbose', is_flag=True)
@pass_context
def list_command(ctx, path, verbose):
    """List ZooKeeper nodes.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    nodes = list_zk_nodes(ctx, path, verbose=verbose)
    if verbose:
        print_response(ctx, nodes, format='table')
    else:
        print('\n'.join(nodes))


@zookeeper_group.command(name='stat')
@argument('path')
@pass_context
def stat_command(ctx, path):
    """Show statistics for ZooKeeper node.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    print(get_zk_node_acls(ctx, path)[1])


@zookeeper_group.command(name='delete')
@argument('path')
@pass_context
def delete_command(ctx, path):
    """Delete ZooKeeper node.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    delete_zk_node(ctx, path)


@zookeeper_group.command(name='create')
@argument('paths', nargs=-1)
@pass_context
def create_command(ctx, paths):
    """Create ZooKeeper nodes, accepts one or more paths.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    create_zk_nodes(ctx, paths)
