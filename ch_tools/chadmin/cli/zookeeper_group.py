import re
import sys

from click import argument, group, option, pass_context
from kazoo.security import make_digest_acl

from ch_tools.chadmin.internal.table_replica import get_table_replica
from ch_tools.chadmin.internal.zookeeper import (
    check_zk_node,
    create_zk_nodes,
    delete_zk_nodes,
    get_zk_node,
    get_zk_node_acls,
    list_zk_nodes,
    update_acls_zk_node,
    update_zk_nodes,
)
from ch_tools.common.cli.formatting import print_json, print_response
from ch_tools.common.cli.parameters import ListParamType, StringParamType


@group("zookeeper")
@option("--port", help="ZooKeeper port.", type=int, default=2181)
@option("--host", help="ZooKeeper host.", type=str)
@option("--timeout", help="ZooKeeper timeout.", default=10)
@option(
    "--zkcli-identity",
    help="Identity for zookeeper cli shell. In a format login:password. "
    "Example: clickhouse:X7ui1dXIXXXXXXXXXXXXXXXXXXXXXXXX",
    type=str,
)
@option(
    "--no-chroot",
    is_flag=True,
    help="If parameter is True we won't use root from CH config and use ZK absolute root",
    default=False,
)
@option(
    "--no-ch-config",
    is_flag=True,
    help="Do not try to get parameters from clickhouse config.xml.",
    default=False,
)
@pass_context
def zookeeper_group(ctx, host, port, timeout, zkcli_identity, no_chroot, no_ch_config):
    """ZooKeeper management commands.

    ZooKeeper command runs client which connects to Zookeeper node.
    By default, it parses config on ClickHouse node in default config location and figures out host and port.
    You can override some of these parameters, but others will be figured out from configs.
    """

    ctx.obj["zk_client_args"] = {
        "port": port,
        "host": host,
        "timeout": timeout,
        "zkcli_identity": zkcli_identity,
        "no_chroot": no_chroot,
        "no_ch_config": no_ch_config,
    }


@zookeeper_group.command(name="get")
@argument("path")
@option("-b", "--binary", is_flag=True)
@pass_context
def get_command(ctx, path, binary):
    """Get ZooKeeper node.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    print(get_zk_node(ctx, path, binary=binary))


@zookeeper_group.command(name="exists")
@argument("path")
@pass_context
def exists_command(ctx, path):
    """Check ZooKeeper node exists or not.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    if check_zk_node(ctx, path):
        print(True)
        return
    print(False)
    sys.exit(1)


@zookeeper_group.command(name="get-acl")
@argument("path")
@pass_context
def get_acl_command(ctx, path):
    """Show node's ACL by path.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    print_json(ctx, get_zk_node_acls(ctx, path)[0])


@zookeeper_group.command(name="list")
@argument("path")
@option("-v", "--verbose", is_flag=True, help="Verbose mode.")
@pass_context
def list_command(ctx, path, verbose):
    """List ZooKeeper nodes.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    nodes = list_zk_nodes(ctx, path, verbose=verbose)
    if verbose:
        print_response(ctx, nodes, format_="table")
    else:
        print("\n".join(nodes))


@zookeeper_group.command(name="stat")
@argument("path")
@pass_context
def stat_command(ctx, path):
    """Show statistics for ZooKeeper node.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    print(get_zk_node_acls(ctx, path)[1])


@zookeeper_group.command(name="create")
@argument("paths", type=ListParamType())
@argument("value", type=StringParamType(), required=False)
@option(
    "--make-parents",
    is_flag=True,
    help="Will create all missing parent nodes.",
    default=False,
)
@pass_context
def create_command(ctx, paths, value, make_parents):
    """Create one or several ZooKeeper nodes.

    Node path can be specified with ClickHouse macros (e.g. "/test_table/{shard}/replicas/{replica}").
    Multiple values can be specified through a comma.
    """
    create_zk_nodes(ctx, paths, value, make_parents=make_parents)


@zookeeper_group.command(name="update")
@argument("paths", type=ListParamType())
@argument("value", type=StringParamType())
@pass_context
def update_command(ctx, paths, value):
    """Update one or several ZooKeeper nodes.

    Node path can be specified with ClickHouse macros (e.g. "/test_table/{shard}/replicas/{replica}").
    Multiple values can be specified through a comma.
    """
    update_zk_nodes(ctx, paths, value)


@zookeeper_group.command(name="delete")
@argument("paths", type=ListParamType())
@pass_context
def delete_command(ctx, paths):
    """Delete one or several ZooKeeper nodes.

    Node path can be specified with ClickHouse macros (e.g. "/test_table/{shard}/replicas/{replica}").
    Multiple values can be specified through a comma.
    """
    delete_zk_nodes(ctx, paths)


@zookeeper_group.command(name="get-table-metadata")
@argument("database")
@argument("table")
@pass_context
def get_table_metadata_command(ctx, database, table):
    """Get table metadata stored in ZooKeeper."""
    table_replica = get_table_replica(ctx, database, table)
    path = table_replica["zookeeper_path"] + "/metadata"
    print(get_zk_node(ctx, path))


@zookeeper_group.command(name="update-table-metadata")
@argument("database")
@argument("table")
@argument("value", type=StringParamType())
@pass_context
def update_table_metadata_command(ctx, database, table, value):
    """Update table metadata stored in ZooKeeper."""
    table_replica = get_table_replica(ctx, database, table)
    table_path = table_replica["zookeeper_path"]
    metadata_paths = [f"{table_path}/metadata"]
    for path in list_zk_nodes(ctx, table_replica["zookeeper_path"] + "/replicas"):
        metadata_paths.append(f"{path}/metadata")

    update_zk_nodes(ctx, metadata_paths, value)


@zookeeper_group.command(name="update-acl")
@argument("path")
@argument("acls", type=ListParamType())
@pass_context
def update_acls_command(ctx, path, acls):
    """Update node's ACLs on specified path by acls in format: bob:q1w2e3:cdrwa,rob:a9s8d7:all.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """

    def _parse_acl(acl):
        splitted = acl.split(":")
        if len(splitted) != 3:
            ctx.fail("Invalid --acls parameter format. See --help for valid format.")

        username, password, short_acl = splitted

        if short_acl == "all":
            return make_digest_acl(username, password, all=True)

        if not re.search("^[rwcda]+$", short_acl):
            ctx.fail(
                "Only `r`, `w`, `c`, `d` or `a` are allowed in acl for read, write, create, delete or admin permissions."
            )

        return make_digest_acl(
            username,
            password,
            read="r" in short_acl,
            write="w" in short_acl,
            create="c" in short_acl,
            delete="d" in short_acl,
            admin="a" in short_acl,
        )

    parsed_acls = [_parse_acl(a) for a in acls]

    update_acls_zk_node(ctx, path, parsed_acls)


@zookeeper_group.command(name="get-table-replica-metadata")
@argument("database")
@argument("table")
@pass_context
def get_table_replica_metadata_command(ctx, database, table):
    """Get table replica metadata stored in ZooKeeper."""
    table_replica = get_table_replica(ctx, database, table)
    path = table_replica["replica_path"] + "/metadata"
    print(get_zk_node(ctx, path))


@zookeeper_group.command(name="get-ddl-task")
@argument("task")
@pass_context
def get_ddl_task_command(ctx, task):
    """Get DDL queue task metadata stored in ZooKeeper."""
    path = f"/clickhouse/task_queue/ddl/{task}"
    print(get_zk_node(ctx, path))


@zookeeper_group.command(name="delete-ddl-task")
@argument("tasks", type=ListParamType())
@pass_context
def delete_ddl_task_command(ctx, tasks):
    """Delete one or several DDL queue task from ZooKeeper.

    Multiple values can be specified through a comma.
    """
    paths = [f"/clickhouse/task_queue/ddl/{task}" for task in tasks]
    delete_zk_nodes(ctx, paths)
