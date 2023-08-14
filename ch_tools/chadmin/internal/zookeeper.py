import logging
import os
from contextlib import contextmanager

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

from ch_tools.chadmin.cli import get_config, get_macros


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


def list_zk_nodes(ctx, path, verbose=False):
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
    with zk_client(ctx) as zk:
        for path in paths:
            path = _format_path(ctx, path)
            print(f"Deleting ZooKeeper node {path}")
            zk.delete(path, recursive=True)


def _format_path(ctx, path):
    args = ctx.obj.get("zk_client_args", {})
    no_ch_config = args.get("no_ch_config", False)
    if no_ch_config:
        return path
    return path.format_map(get_macros(ctx))


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

    if no_ch_config:
        if not host:
            host = "localhost"
        connect_str = f"{host}:{port}"
    else:
        # Intentionally don't try to load preprocessed.
        # We are not sure here if zookeeper-servers's changes already have been reloaded by CH.
        zk_config = get_config(ctx, try_preprocessed=False).zookeeper
        connect_str = ",".join(
            f'{host if host else node["host"]}:{port if port else node["port"]}'
            for node in zk_config.nodes
        )
        if not no_chroot and zk_config.root is not None:
            connect_str += zk_config.root

        if zkcli_identity is None:
            zkcli_identity = zk_config.identity

    auth_data = None
    if zkcli_identity is not None:
        auth_data = [("digest", zkcli_identity)]

    return KazooClient(
        connect_str, auth_data=auth_data, timeout=timeout, logger=logging.getLogger()
    )
