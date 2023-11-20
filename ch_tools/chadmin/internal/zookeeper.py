import logging
import os
import re
from contextlib import contextmanager
from queue import Queue

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, NotEmptyError

from ch_tools.chadmin.cli import get_clickhouse_config, get_macros


def get_zk_node(ctx, path, binary=False):
    with zk_client(ctx) as zk:
        path = _format_path(ctx, path)
        value = zk.get(path)[0]
        return value if binary else value.decode().strip()


def check_zk_node(ctx, path):
    with zk_client(ctx) as zk:
        path = _format_path(ctx, path)
        return zk.exists(path)


def get_zk_node_acls(ctx, path):
    with zk_client(ctx) as zk:
        path = _format_path(ctx, path)
        return zk.get_acls(path)


def _get_children(zk, path):
    try:
        return zk.get_children(path)
    except NoNodeError:
        return []  # in the case ZK deletes a znode while we traverse the tree


def list_zk_nodes(ctx, path, verbose=False):
    def _stat_node(zk, node):
        descendants_count = 0
        queue = [node]
        while queue:
            item = queue.pop()
            children = _get_children(zk, item)
            descendants_count += len(children)
            queue.extend(os.path.join(item, node) for node in children)

        return {
            "path": node,
            "nodes": descendants_count,
        }

    with zk_client(ctx) as zk:
        path = _format_path(ctx, path)
        result = zk.get_children(path)
        nodes = [os.path.join(path, node) for node in sorted(result)]
        return [_stat_node(zk, node) for node in nodes] if verbose else nodes


def create_zk_nodes(ctx, paths, value=None, make_parents=False):
    if isinstance(value, str):
        value = value.encode()
    else:
        value = b""

    with zk_client(ctx) as zk:
        for path in paths:
            zk.create(_format_path(ctx, path), value, makepath=make_parents)


def update_zk_nodes(ctx, paths, value):
    if isinstance(value, str):
        value = value.encode()

    with zk_client(ctx) as zk:
        for path in paths:
            zk.set(_format_path(ctx, path), value)


def update_acls_zk_node(ctx, path, acls):
    with zk_client(ctx) as zk:
        zk.set_acls(_format_path(ctx, path), acls)


def delete_zk_node(ctx, path):
    delete_zk_nodes(ctx, [path])


def delete_zk_nodes(ctx, paths):
    paths_formated = [_format_path(ctx, path) for path in paths]
    with zk_client(ctx) as zk:
        _delete_zk_nodes(zk, paths_formated)


def _delete_zk_nodes(zk, paths):
    for path in paths:
        print(f"Deleting ZooKeeper node {path}")
        zk.delete(path, recursive=True)


def _format_path(ctx, path):
    args = ctx.obj.get("zk_client_args", {})
    no_ch_config = args.get("no_ch_config", False)
    if no_ch_config:
        return path
    return path.format_map(get_macros(ctx))


def _set_node_value(zk, path, value):
    """
    Set value to node in zk.
    """
    if zk.exists(path):
        try:
            zk.set(path, value.encode())
        except NoNodeError:
            print(f"Can not set for node: {path}  value : {value}")


def _find_paths(zk, root_path, included_paths_regexp, excluded_paths_regexp=None):
    """
    Traverse zookeeper tree from root_path with bfs approach.

    Return paths of nodes that match the include regular expression and do not match the excluded one.
    """
    paths = set()
    queue: Queue = Queue()
    queue.put(root_path)
    included_regexp = re.compile("|".join(included_paths_regexp))
    excluded_regexp = (
        re.compile("|".join(excluded_paths_regexp)) if excluded_paths_regexp else None
    )

    while not queue.empty():
        path = queue.get()
        if excluded_regexp and re.match(excluded_regexp, path):
            continue

        for child_node in _get_children(zk, path):
            subpath = os.path.join(path, child_node)

            if re.match(included_regexp, subpath):
                paths.add(subpath)
            else:
                queue.put(os.path.join(path, subpath))

    return list(paths)


def clean_zk_metadata_for_hosts(ctx, nodes):
    """
    Perform cleanup in zookeeper after deleting hosts in the cluster or whole cluster deleting.
    """

    def _try_delete_zk_node(zk, node):
        try:
            _delete_zk_nodes(zk, [node])
        except NoNodeError:
            #  Someone deleted node before us. Do nothing.
            print("Node {node} is already absent, skipped".format(node=node))
        except NotEmptyError:
            #  Someone created child node while deleting.
            #  I'm not sure that we can get this exception with recursive=True.
            #  Do nothing.
            print("Node {node} is not empty, skipped".format(node=node))

    def _find_tables(zk, root_path, nodes):
        """
        Find nodes mentions in zk for table management.
        """
        hosts_metions = _find_paths(zk, root_path, [".*/" + node for node in nodes])
        excluded_paths = [
            ".*clickhouse/task_queue",
            ".*clickhouse/zero_copy",
        ]
        included_paths = [".*/replicas/" + node for node in nodes]

        included_regexp = re.compile("|".join(included_paths))
        excluded_regexp = re.compile("|".join(excluded_paths))
        table_paths = list(
            filter(
                lambda path: not re.match(excluded_regexp, path)
                and re.match(included_regexp, path),
                hosts_metions,
            )
        )

        # Paths will be like */shard1/replicas/hostname. But we need */shard1.
        # Go up for 2 directories.
        table_paths = [os.sep.join(path.split(os.sep)[:-2]) for path in table_paths]
        # One path might be in list several times. Make list unique.
        return (list(set(table_paths)), hosts_metions)

    def _find_databases(zk, root_path, nodes):
        """
        Databases contains replicas in zk in cases when engine is Replicated with
        pattern: <shard>|<replica>
        """
        hosts_mentions = _find_paths(
            zk, root_path, [".*/.*[|]" + node for node in nodes]
        )

        included_paths = [".*/replicas/.*[|]" + node for node in nodes]
        included_regexp = re.compile("|".join(included_paths))

        databases_paths = list(
            filter(lambda path: re.match(included_regexp, path), hosts_mentions)
        )
        databases_paths = [
            os.sep.join(path.split(os.sep)[:-2]) for path in hosts_mentions
        ]

        return (list(set(databases_paths)), hosts_mentions)

    def _set_replicas_is_lost(zk, table_paths, nodes):
        """
        Set flag <path>/replicas/<replica_name>/is_lost to 1
        """
        for path in table_paths:

            replica_path = os.path.join(path, "replicas")
            if not zk.exists(replica_path):
                continue

            for node in nodes:
                is_lost_flag_path = os.path.join(replica_path, node, "is_lost")
                print("Set is_lost_flag " + is_lost_flag_path)
                _set_node_value(zk, is_lost_flag_path, "1")

    def _remove_replicas_queues(zk, paths, replica_names):
        """
        Remove <path>/replicas/<replica_name>/queue
        """
        for path in paths:
            replica_name = os.path.join(path, "replicas")
            if not zk.exists(replica_name):
                continue

            for replica_name in replica_names:
                queue_path = os.path.join(replica_name, replica_name, "queue")
                if not zk.exists(queue_path):
                    continue

                if zk.exists(queue_path):
                    _try_delete_zk_node(zk, queue_path)

    def _remove_hosts_nodes(zk, paths):
        for node in paths:
            _try_delete_zk_node(zk, node)

    def _remove_if_no_hosts_in_replicas(zk, paths):
        """
        Remove node if subnode is empty
        """
        for path in paths:
            child_full_path = os.path.join(path, "replicas")
            if (
                not zk.exists(child_full_path)
                or len(_get_children(zk, child_full_path)) == 0
            ):
                _try_delete_zk_node(zk, path)

    def tables_cleanup(zk, zk_root_path):
        """
        Do cleanup for tables which contains nodes.
        """
        (table_paths, hosts_paths) = _find_tables(zk, zk_root_path, nodes)
        _set_replicas_is_lost(zk, table_paths, nodes)
        _remove_replicas_queues(zk, table_paths, nodes)
        _remove_hosts_nodes(zk, hosts_paths)
        _remove_if_no_hosts_in_replicas(zk, table_paths)

    def databases_cleanups(zk, zk_root_path):
        """
        Do cleanup for databases which contains nodes.
        """

        (databases_paths, hosts_paths) = _find_databases(zk, zk_root_path, nodes)
        _remove_hosts_nodes(zk, hosts_paths)
        _remove_if_no_hosts_in_replicas(zk, databases_paths)

    zk_root_path = _format_path(ctx, "/")
    with zk_client(ctx) as zk:
        tables_cleanup(zk, zk_root_path)
        databases_cleanups(zk, zk_root_path)


@contextmanager
def zk_client(ctx):
    zk = _get_zk_client(ctx)
    try:
        zk.start()
        yield zk
    finally:
        zk.stop()


def _get_zk_client(ctx):
    """
    Create and return KazooClient.
    """
    args = ctx.obj.get("zk_client_args", {})
    host = args.get("host")
    port = args.get("port", 2181)
    timeout = args.get("timeout", 10)
    zkcli_identity = args.get("zkcli_identity")
    no_chroot = args.get("no_chroot", False)
    no_ch_config = args.get("no_ch_config", False)
    zk_root_path = args.get("zk_root_path", None)

    if no_ch_config:
        if not host:
            host = "localhost"
        connect_str = f"{host}:{port}"
    else:
        # Intentionally don't try to load preprocessed.
        # We are not sure here if zookeeper-servers's changes already have been reloaded by CH.
        zk_config = get_clickhouse_config(ctx, try_preprocessed=False).zookeeper
        connect_str = ",".join(
            f'{host if host else node["host"]}:{port if port else node["port"]}'
            for node in zk_config.nodes
        )
        if zk_root_path:
            connect_str += zk_root_path
        elif not no_chroot and zk_config.root is not None:
            connect_str += zk_config.root

        if zkcli_identity is None:
            zkcli_identity = zk_config.identity

    auth_data = None
    if zkcli_identity is not None:
        auth_data = [("digest", zkcli_identity)]

    return KazooClient(
        connect_str, auth_data=auth_data, timeout=timeout, logger=logging.getLogger()
    )
