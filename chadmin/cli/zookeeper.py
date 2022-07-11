from click import argument, group, option, pass_context

from cloud.mdb.cli.common.formatting import print_response
from cloud.mdb.cli.common.parameters import ListParamType, StringParamType
from cloud.mdb.clickhouse.tools.chadmin.internal.table_replica import get_table_replica
from cloud.mdb.clickhouse.tools.chadmin.internal.zookeeper import (
    create_zk_nodes,
    delete_zk_node,
    get_zk_node,
    get_zk_node_acls,
    list_zk_nodes,
    update_zk_nodes,
)


@group('zookeeper')
@option('--port', help='ZooKeeper port.', type=int, default=2181)
@option('--host', help='ZooKeeper host.', type=str)
@option(
    '--zkcli_identity',
    help='Identity for zookeeper cli shell. In a format login:password. '
    'Example: clickhouse:X7ui1dXIXXXXXXXXXXXXXXXXXXXXXXXX',
    type=str,
)
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
@option('-b', '--binary', is_flag=True)
@pass_context
def get_command(ctx, path, binary):
    """Get ZooKeeper node.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    print(get_zk_node(ctx, path, binary=binary))


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


@zookeeper_group.command(name='create')
@argument('paths', type=ListParamType())
@argument('value', type=StringParamType(), required=False)
@pass_context
def create_command(ctx, paths, value):
    """Create one or several ZooKeeper nodes.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    create_zk_nodes(ctx, paths, value)


@zookeeper_group.command(name='update')
@argument('paths', type=ListParamType())
@argument('value', type=StringParamType())
@pass_context
def update_command(ctx, paths, value):
    """Update one or several ZooKeeper node values.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    update_zk_nodes(ctx, paths, value)


@zookeeper_group.command(name='delete')
@argument('path')
@pass_context
def delete_command(ctx, path):
    """Delete ZooKeeper node.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    delete_zk_node(ctx, path)


@zookeeper_group.command(name='get-table-metadata')
@argument('database')
@argument('table')
@pass_context
def get_table_metadata_command(ctx, database, table):
    """Get table metadata stored in ZooKeeper."""
    table_replica = get_table_replica(ctx, database, table)
    path = table_replica['zookeeper_path'] + '/metadata'
    print(get_zk_node(ctx, path))


@zookeeper_group.command(name='update-table-metadata')
@argument('database')
@argument('table')
@argument('value', type=StringParamType())
@pass_context
def update_table_metadata_command(ctx, database, table, value):
    """Update table metadata stored in ZooKeeper."""
    table_replica = get_table_replica(ctx, database, table)
    table_path = table_replica['zookeeper_path']
    metadata_paths = [f'{table_path}/metadata']
    for path in list_zk_nodes(ctx, table_replica['zookeeper_path'] + '/replicas'):
        metadata_paths.append(f'{path}/metadata')

    update_zk_nodes(ctx, metadata_paths, value)


@zookeeper_group.command(name='get-table-replica-metadata')
@argument('database')
@argument('table')
@pass_context
def get_table_replica_metadata_command(ctx, database, table):
    """Get table replica metadata stored in ZooKeeper."""
    table_replica = get_table_replica(ctx, database, table)
    path = table_replica['replica_path'] + '/metadata'
    print(get_zk_node(ctx, path))
