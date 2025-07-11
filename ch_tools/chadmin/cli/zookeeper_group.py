import os
import re
import sys
from typing import Any, Optional

from cloup import (
    Context,
    argument,
    constraint,
    group,
    option,
    option_group,
    pass_context,
)
from cloup.constraints import If, IsSet, RequireAtLeast, require_all
from kazoo.security import make_digest_acl

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.table_replica import get_table_replica
from ch_tools.chadmin.internal.zookeeper import (
    _get_zero_copy_zookeeper_path,
    check_zk_node,
    create_zk_nodes,
    delete_recursive,
    delete_zk_nodes,
    find_paths,
    get_children,
    get_zk_node,
    get_zk_node_acls,
    list_zk_nodes,
    update_acls_zk_node,
    update_zk_nodes,
    zk_client,
)
from ch_tools.chadmin.internal.zookeeper_clean import (
    clean_zk_metadata_for_hosts,
    delete_zero_copy_locks_for_replica,
)
from ch_tools.common import logging
from ch_tools.common.cli.formatting import print_json, print_response
from ch_tools.common.cli.parameters import ListParamType, StringParamType
from ch_tools.common.config import load_config


@group("zookeeper", cls=Chadmin)
@option("--port", help="ZooKeeper port.", type=int, default=2181)
@option("--host", help="ZooKeeper host.", type=str)
@option("--secure", help="Use secure connection.", default=False, is_flag=True)
@option(
    "--verify-ssl-certs/--no-verify-ssl-certs",
    help="Check or not SSL Certificates in secure connection.",
    default=True,
)
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
@option(
    "-c",
    "--chroot",
    "zk_root_path",
    type=str,
    help="Cluster ZooKeeper root path. If not specified,the root path will be used.",
    required=False,
)
@pass_context
def zookeeper_group(
    ctx: Context,
    host: str,
    secure: bool,
    verify_ssl_certs: bool,
    port: int,
    timeout: int,
    zkcli_identity: str,
    no_chroot: bool,
    no_ch_config: bool,
    zk_root_path: str,
) -> None:
    """ZooKeeper management commands.

    ZooKeeper command runs client which connects to Zookeeper node.
    By default, it parses config on ClickHouse node in default config location and figures out host and port.
    You can override some of these parameters, but others will be figured out from configs.
    """

    ctx.obj["zk_client_args"] = {
        "port": port,
        "host": host,
        "use_ssl": secure,
        "verify_ssl_certs": verify_ssl_certs,
        "timeout": timeout,
        "zkcli_identity": zkcli_identity,
        "no_chroot": no_chroot,
        "no_ch_config": no_ch_config,
        "zk_root_path": zk_root_path,
    }


@zookeeper_group.command("get")
@argument("path")
@option("-b", "--binary", is_flag=True)
@pass_context
def get_command(ctx: Context, path: str, binary: bool) -> None:
    """Get ZooKeeper node.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    logging.info(get_zk_node(ctx, path, binary=binary))


@zookeeper_group.command("exists")
@argument("path")
@pass_context
def exists_command(ctx: Context, path: str) -> None:
    """Check ZooKeeper node exists or not.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    if check_zk_node(ctx, path):
        logging.info(True)
        return
    logging.error(False)
    sys.exit(1)


@zookeeper_group.command("get-acl")
@argument("path")
@pass_context
def get_acl_command(ctx: Context, path: str) -> None:
    """Show node's ACL by path.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    print_json(ctx, get_zk_node_acls(ctx, path)[0])


@zookeeper_group.command("list")
@argument("path")
@option("-v", "--verbose", is_flag=True, help="Verbose mode.")
@pass_context
def list_command(ctx: Context, path: str, verbose: bool) -> None:
    """List ZooKeeper nodes.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    nodes = list_zk_nodes(ctx, path, verbose=verbose)
    if verbose:
        print_response(ctx, nodes, format_="table")
    else:
        logging.info("\n".join(nodes))


@zookeeper_group.command("stat")
@argument("path")
@pass_context
def stat_command(ctx: Context, path: str) -> None:
    """Show statistics for ZooKeeper node.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """
    logging.info(get_zk_node_acls(ctx, path)[1])


@zookeeper_group.command("create")
@argument("paths", type=ListParamType())
@argument("value", type=StringParamType(), required=False)
@option(
    "--make-parents",
    is_flag=True,
    help="Will create all missing parent nodes.",
    default=False,
)
@pass_context
def create_command(ctx: Context, paths: list, value: str, make_parents: bool) -> None:
    """Create one or several ZooKeeper nodes.

    Node path can be specified with ClickHouse macros (e.g. "/test_table/{shard}/replicas/{replica}").
    Multiple values can be specified through a comma.
    """
    create_zk_nodes(ctx, paths, value, make_parents=make_parents)


@zookeeper_group.command("update")
@argument("paths", type=ListParamType())
@argument("value", type=StringParamType())
@pass_context
def update_command(ctx: Context, paths: list, value: str) -> None:
    """Update one or several ZooKeeper nodes.

    Node path can be specified with ClickHouse macros (e.g. "/test_table/{shard}/replicas/{replica}").
    Multiple values can be specified through a comma.
    """
    update_zk_nodes(ctx, paths, value)


@zookeeper_group.command("delete")
@argument("paths", type=ListParamType())
@pass_context
def delete_command(ctx: Context, paths: list) -> None:
    """Delete one or several ZooKeeper nodes.

    Node path can be specified with ClickHouse macros (e.g. "/test_table/{shard}/replicas/{replica}").
    Multiple values can be specified through a comma.
    """
    delete_zk_nodes(ctx, paths)


@zookeeper_group.command("get-table-metadata")
@argument("database_name", metavar="DATABASE")
@argument("table_name", metavar="TABLE")
@pass_context
def get_table_metadata_command(
    ctx: Context, database_name: str, table_name: str
) -> None:
    """Get table metadata stored in ZooKeeper."""
    table_replica = get_table_replica(ctx, database_name, table_name)
    path = table_replica["zookeeper_path"] + "/metadata"
    logging.info(get_zk_node(ctx, path))


@zookeeper_group.command("update-table-metadata")
@argument("database_name", metavar="DATABASE")
@argument("table_name", metavar="TABLE")
@argument("value", type=StringParamType())
@pass_context
def update_table_metadata_command(
    ctx: Context, database_name: str, table_name: str, value: str
) -> None:
    """Update table metadata stored in ZooKeeper."""
    table_replica = get_table_replica(ctx, database_name, table_name)
    table_path = table_replica["zookeeper_path"]
    metadata_paths = [f"{table_path}/metadata"]
    for path in list_zk_nodes(ctx, table_replica["zookeeper_path"] + "/replicas"):
        metadata_paths.append(f"{path}/metadata")

    update_zk_nodes(ctx, metadata_paths, value)


@zookeeper_group.command("update-acl")
@argument("path")
@argument("acls", type=ListParamType())
@pass_context
def update_acls_command(ctx: Context, path: str, acls: list) -> None:
    """Update node's ACLs on specified path by acls in format: bob:q1w2e3:cdrwa,rob:a9s8d7:all.

    Node path can be specified with ClickHouse macros. Example: "/test_table/{shard}/replicas/{replica}".
    """

    def _parse_acl(acl: str) -> Any:
        acl_tuple = acl.split(":")
        if len(acl_tuple) != 3:
            ctx.fail("Invalid --acls parameter format. See --help for valid format.")

        username, password, short_acl = acl_tuple

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


@zookeeper_group.command("get-table-replica-metadata")
@argument("database")
@argument("table")
@pass_context
def get_table_replica_metadata_command(ctx: Context, database: str, table: str) -> None:
    """Get table replica metadata stored in ZooKeeper."""
    table_replica = get_table_replica(ctx, database, table)
    path = table_replica["replica_path"] + "/metadata"
    logging.info(get_zk_node(ctx, path))


@zookeeper_group.command("get-ddl-task")
@argument("task")
@pass_context
def get_ddl_task_command(ctx: Context, task: str) -> None:
    """Get DDL queue task metadata stored in ZooKeeper."""
    path = f"/clickhouse/task_queue/ddl/{task}"
    logging.info(get_zk_node(ctx, path))


@zookeeper_group.command("delete-ddl-task")
@argument("tasks", type=ListParamType())
@pass_context
def delete_ddl_task_command(ctx: Context, tasks: list) -> None:
    """Delete one or several DDL queue task from ZooKeeper.

    Multiple values can be specified through a comma.
    """
    paths = [f"/clickhouse/task_queue/ddl/{task}" for task in tasks]
    delete_zk_nodes(ctx, paths)


@zookeeper_group.command(
    name="cleanup-removed-hosts-metadata",
    help="Remove metadata from Zookeeper for specified hosts.",
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@option(
    "--clean-ddl-queue/--no-clean-ddl-queue",
    is_flag=True,
    default=True,
    help="Perform ddl query cleanup.",
    type=bool,
)
@argument("fqdn", type=ListParamType())
@pass_context
def clickhouse_hosts_command(
    ctx: Context, fqdn: list, clean_ddl_queue: bool, dry_run: bool
) -> None:
    # We can't get the ddl queue path from clickhouse config,
    # because in some cases we are changing this path while performing cluster resetup.
    config = load_config()
    clean_zk_metadata_for_hosts(
        ctx,
        fqdn,
        cleanup_ddl_queue=clean_ddl_queue,
        zk_ddl_query_path=config["clickhouse"]["distributed_ddl_path"],
        dry_run=dry_run,
    )
    for replica in fqdn:
        clean_zk_locks_command(
            ctx,
            replica=replica,
            dry_run=dry_run,
        )


@zookeeper_group.command(
    name="remove-hosts-from-table",
    help="Remove hosts from table metadata in the Zookeeper.",
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@argument("zookeeper-table-path")
@argument("fqdn", type=ListParamType())
@pass_context
def remove_hosts_from_table(
    ctx: Context, zookeeper_table_path: str, fqdn: list, dry_run: bool
) -> None:
    clean_zk_metadata_for_hosts(
        ctx,
        fqdn,
        zk_ddl_query_path=zookeeper_table_path,
        cleanup_database=False,
        cleanup_ddl_queue=False,
        dry_run=dry_run,
    )
    for replica in fqdn:
        clean_zk_locks_command(
            ctx,
            replica=replica,
            dry_run=dry_run,
        )


@zookeeper_group.command("cleanup-zero-copy-locks")
@option(
    "--zero_copy_path",
    "zero_copy_path",
    default=None,
    help=(
        "Path to zero-copy related data in ZooKeeper."
        "Will use 'remote_fs_zero_copy_zookeeper_path' value from ClickHouse by default."
    ),
)
@option_group(
    "Cleaning scope selection selection options",
    option(
        "-t",
        "--table-uuid",
        "table_uuid",
        default=None,
        help=("UUID of a table to clean."),
    ),
    option(
        "-p",
        "--part-id",
        "part_id",
        default=None,
        help=("Part id to clean. Also requires table to be specified."),
    ),
    constraint=If(IsSet("part_id"), then=require_all),
)
@option(
    "-r",
    "--replica",
    "replica",
    default=None,
    help=("Replica name to clean."),
)
@constraint(
    RequireAtLeast(1),
    ["table_uuid", "part_id", "replica"],
)
@option(
    "--dry-run",
    "dry_run",
    is_flag=True,
    help=("Do not delete objects. Show only statistics."),
)
@pass_context
def clean_zk_locks_command(
    ctx: Context,
    zero_copy_path: Optional[str] = None,
    table_uuid: Optional[str] = None,
    part_id: Optional[str] = None,
    replica: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    """
    Clean zero copy locks.
    """
    zero_copy_path = zero_copy_path or _get_zero_copy_zookeeper_path(ctx)

    with zk_client(ctx) as zk:
        if replica:
            delete_zero_copy_locks_for_replica(
                zk, zero_copy_path, table_uuid, part_id, replica, dry_run
            )
        else:
            # No need to find every replica's path. Removing part's or table's directory is enough.
            if part_id:
                template = re.escape(rf"{zero_copy_path}/{table_uuid}/{part_id}")
                paths = find_paths(zk, zero_copy_path, [template])
                if not paths:
                    return
                table_path = os.path.dirname(paths[0])
                # Checking if we can just delete the table's directory instead
                if len(get_children(zk, table_path)) == 1:
                    paths = [table_path]
            else:
                template = re.escape(rf"{zero_copy_path}/{table_uuid}")
                paths = find_paths(zk, zero_copy_path, [template])

            delete_recursive(zk, paths, dry_run)
