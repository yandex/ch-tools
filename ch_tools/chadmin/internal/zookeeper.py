"""
ZooKeeper utilities for ClickHouse administration.

Provides tools for managing ZooKeeper nodes, transactions, and client connections.
Includes transaction builder for atomic operations, path formatting with macro support,
and optimized recursive deletion for large node hierarchies.
"""

import os
import re
from collections import deque
from contextlib import contextmanager
from math import sqrt
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Set,
    Union,
)

from click import Context
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError, NotEmptyError
from kazoo.protocol.states import ZnodeStat

from ch_tools.chadmin.internal.utils import chunked, replace_macros
from ch_tools.common import logging
from ch_tools.common.clickhouse.config import get_clickhouse_config, get_macros
from ch_tools.common.clickhouse.config.clickhouse import ClickhouseConfig


class ZKTransactionBuilder:
    """
    Builder for ZooKeeper transactions with path tracking.

    Provides methods to create and delete nodes within a transaction,
    tracks all operations, and validates results on commit.

    Supports context manager protocol for automatic cleanup.

    Example:
        with ZKTransactionBuilder(ctx, zk) as builder:
            builder.create_node("/path1", "value1")
            builder.create_node("/path2", "value2")
            builder.commit()
    """

    def __init__(self, ctx: Context, zk: KazooClient) -> None:
        self.ctx = ctx
        self.zk = zk
        self.txn = zk.transaction()
        self.path_to_nodes: List[str] = []
        self._committed = False

    def __enter__(self) -> "ZKTransactionBuilder":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager, reset state."""
        self.reset()

    def create_node(self, path: str, value: str = "") -> "ZKTransactionBuilder":
        """Add create operation to transaction. Returns self for method chaining."""
        if self._committed:
            raise RuntimeError("Cannot add operations to committed transaction")
        self.path_to_nodes.append(path)
        self.txn.create(path=format_path(self.ctx, path), value=value.encode())
        return self

    def delete_node(self, path: str) -> "ZKTransactionBuilder":
        """Add delete operation to transaction. Returns self for method chaining."""
        if self._committed:
            raise RuntimeError("Cannot add operations to committed transaction")
        self.path_to_nodes.append(path)
        self.txn.delete(path=format_path(self.ctx, path))
        return self

    def commit(self) -> None:
        """Execute transaction and validate results."""
        if self._committed:
            raise RuntimeError("Transaction already committed")

        result = self.txn.commit()
        self._committed = True

        # Log errors if validation fails (keep original format for external logic)
        if not self._check_result_txn(result, no_throw=True):
            for status in zip(self.path_to_nodes, result):
                logging.error(f"{status}")

        # Raise exception if there were errors
        self._check_result_txn(result, no_throw=False)

    def reset(self) -> None:
        """Reset transaction state for reuse."""
        self.path_to_nodes = []
        self.txn = self.zk.transaction()
        self._committed = False

    @staticmethod
    def _check_result_txn(results: List, no_throw: bool = False) -> bool:
        """
        Validate transaction results.

        Args:
            results: List of transaction results
            no_throw: If True, return False on error instead of raising

        Returns:
            True if all operations succeeded, False otherwise (only if no_throw=True)

        Raises:
            NodeExistsError: If node already exists (only if no_throw=False)
            Exception: Any other exception from transaction (only if no_throw=False)
        """
        for result in results:
            if isinstance(result, NodeExistsError):
                if no_throw:
                    return False
                raise NodeExistsError()
            if isinstance(result, Exception):
                if no_throw:
                    return False
                logging.error(f"Transaction error: {result}, type={type(result)}")
                raise result
        return True


def has_zk() -> bool:
    return not ClickhouseConfig.load().zookeeper.is_empty()


def get_zk_node(ctx: Context, path: str, binary: bool = False) -> str:
    with zk_client(ctx) as zk:
        path = format_path(ctx, path)
        value = zk.get(path)[0]
        return value if binary else value.decode().strip()


def check_zk_node(ctx: Context, path: str) -> ZnodeStat:
    with zk_client(ctx) as zk:
        path = format_path(ctx, path)
        return zk.exists(path)


def get_zk_node_acls(ctx: Context, path: str) -> List[ZnodeStat]:
    with zk_client(ctx) as zk:
        path = format_path(ctx, path)
        return zk.get_acls(path)


def get_children(zk: KazooClient, path: str) -> List[str]:
    try:
        return zk.get_children(path)
    except NoNodeError:
        return []  # in the case ZK deletes a znode while we traverse the tree


def list_children(ctx: Context, path: str) -> List[str]:
    with zk_client(ctx) as zk:
        return get_children(zk, path)


def list_zk_nodes(
    ctx: Context, path: str, verbose: bool = False
) -> Union[List[str], List[Dict[str, Any]]]:
    def _stat_node(zk: KazooClient, node: str) -> Dict[str, Any]:
        descendants_count = 0
        queue = [node]
        while queue:
            item = queue.pop()
            children = get_children(zk, item)
            descendants_count += len(children)
            queue.extend(os.path.join(item, node) for node in children)

        return {
            "path": node,
            "nodes": descendants_count,
        }

    with zk_client(ctx) as zk:
        path = format_path(ctx, path)
        result = zk.get_children(path)
        nodes = [os.path.join(path, node) for node in sorted(result)]
        return [_stat_node(zk, node) for node in nodes] if verbose else nodes


def create_zk_nodes(
    ctx: Context,
    paths: List[str],
    value: Optional[Union[str, bytes]] = None,
    make_parents: bool = False,
    exists_ok: bool = False,
) -> None:
    if isinstance(value, str):
        value = value.encode()
    elif value is None:
        value = b""

    with zk_client(ctx) as zk:
        for path in paths:
            try:
                zk.create(
                    format_path(ctx, path),
                    value,
                    makepath=make_parents,
                )
            except NodeExistsError:
                if exists_ok:
                    return
                raise


def update_zk_nodes(ctx: Context, paths: List[str], value: Union[str, bytes]) -> None:
    if isinstance(value, str):
        value = value.encode()

    with zk_client(ctx) as zk:
        for path in paths:
            zk.set(format_path(ctx, path), value)


def update_acls_zk_node(ctx: Context, path: str, acls: Any) -> None:
    with zk_client(ctx) as zk:
        zk.set_acls(format_path(ctx, path), acls)


def delete_zk_node(ctx: Context, path: str, dry_run: bool = False) -> None:
    delete_zk_nodes(ctx, [path], dry_run)


def delete_zk_nodes(ctx: Context, paths: List[str], dry_run: bool = False) -> None:
    paths_formated = [format_path(ctx, path) for path in paths]
    with zk_client(ctx) as zk:
        delete_recursive(zk, paths_formated, dry_run)


def format_path(ctx: Context, path: str) -> str:
    args = ctx.obj.get("zk_client_args", {})
    no_ch_config = args.get("no_ch_config", False)
    if no_ch_config:
        return path
    return replace_macros(path, get_macros(ctx))


def set_node_value(zk: KazooClient, path: str, value: str) -> None:
    """
    Set value to node in zk.
    """
    if zk.exists(path):
        try:
            zk.set(path, value.encode())
        except NoNodeError:
            logging.warning("Can not set for node: {}  value : {}", path, value)


def find_paths(
    zk: KazooClient,
    root_path: str,
    included_paths_regexp: List[str],
    excluded_paths: Optional[List[str]] = None,
) -> List[str]:
    """
    Traverse zookeeper tree from root_path with bfs approach.

    Return paths of nodes that match the include regular expression and do not match the excluded one.
    """
    paths: Set[str] = set()
    queue: deque[str] = deque([root_path])
    included_regexp = re.compile("|".join(included_paths_regexp))
    excluded_regexp = re.compile("|".join(excluded_paths)) if excluded_paths else None
    while len(queue):
        path = queue.popleft()
        if excluded_regexp and re.match(excluded_regexp, path):
            continue
        for child_node in get_children(zk, path):
            subpath = os.path.join(path, child_node)
            if re.match(included_regexp, subpath):
                paths.add(subpath)
            else:
                queue.append(os.path.join(path, subpath))

    return list(paths)


def find_leafs_and_nodes(
    zk: KazooClient, root_path: str, predicate: Callable
) -> Iterable[str]:
    """
    Recursively traverses zookeeper directory and returns all paths that satisfy the predicate.

    The predicate is applied on the leaf nodes only.
    If all nodes in a directory satisfy the predicate, then path of the node is also returned.
    """

    def _gen_matched_paths(path: str) -> Iterable[str]:
        children = set(get_children(zk, path))
        matched_children = 0

        if not children:
            if predicate(path):
                yield path

        for child in children:
            child_path = os.path.join(path, child)
            for matched_path in _gen_matched_paths(child_path):
                # Check if returned path is a direct children
                matched_path_dir = os.path.dirname(matched_path)
                if path == matched_path_dir:
                    matched_children += 1
                yield matched_path

        if children and matched_children == len(children):
            yield path

    yield from _gen_matched_paths(root_path)


def delete_nodes_transaction(
    zk: KazooClient, to_delete_in_trasaction: List[str]
) -> None:
    """
    Perform deletion for the list of nodes in a single transaction.
    If the transaction fails, go through the list and delete the nodes one by one.
    """
    delete_transaction = zk.transaction()
    for node in to_delete_in_trasaction:
        delete_transaction.delete(node)
    result = delete_transaction.commit()

    if result.count(True) == len(result):
        # Transaction completed successfully, exit.
        return

    logging.info(
        "Delete transaction have failed. Fallthrough to single delete operations for zk_nodes : {}",
        to_delete_in_trasaction,
    )
    for node in to_delete_in_trasaction:
        successful_delete = False
        while not successful_delete:
            try:
                zk.delete(node, recursive=True)
                successful_delete = True
            except NoNodeError:
                #  Someone deleted node before us. Do nothing.
                logging.error("Node {} is already absent, skipped", node)
                successful_delete = True
            except NotEmptyError:
                # Someone created a node while we deleting. Restart the operation.
                pass


def remove_subpaths(paths: List[str]) -> List[str]:
    """
    Removing from the list paths that are subpath of another.

    Example:
    [/a, /a/b/c<-remove it]
    """
    if not paths:
        return []
    # Sorting the list in the lexicographic order
    paths.sort()
    paths_splited = [path.split("/") for path in paths]
    normalized_paths = [paths_splited[0]]
    # If path[i] has subnode path[j] then all paths from i to j will be subnode of i.
    for path in paths_splited:
        last = normalized_paths[-1]
        # Ignore the path if the last normalized one is its prefix
        if len(last) > len(path) or path[: len(last)] != last:
            normalized_paths.append(path)
    return ["/".join(path) for path in normalized_paths]


def delete_recursive(zk: KazooClient, paths: List[str], dry_run: bool = False) -> None:
    """
    Kazoo already has the ability to recursively delete nodes, but the implementation is quite naive
    and has poor performance with a large number of nodes being deleted.

    In this implementation we unite the nodes to delete in transactions to do single operation for batch of nodes.
    To delete in correct order first of all we perform topological sort using bfs approach.
    """

    if len(paths) == 0:
        return

    logging.debug("Node to recursive delete {}", paths)
    paths = remove_subpaths(paths)
    nodes_to_delete = []
    queue = deque(paths)

    while queue:
        path = queue.popleft()
        nodes_to_delete.append(path)
        for child_node in get_children(zk, path):
            queue.append(os.path.join(path, child_node))

    logging.info("Got {} nodes to remove.", len(nodes_to_delete))
    if dry_run:
        logging.info("Would delete nodes: {}", nodes_to_delete)
        return

    # When number of nodes to delete is large preferable to use greater transaction size.
    operations_in_transaction = max(100, int(sqrt(len(nodes_to_delete))))

    for transaction_orerations in chunked(
        reversed(nodes_to_delete), operations_in_transaction
    ):
        delete_nodes_transaction(zk, transaction_orerations)


def escape_for_zookeeper(s: str) -> str:
    # clickhouse uses name formatting in zookeeper.
    # See escapeForFileName.cpp
    result = []
    for c in s:
        if c.isalnum() or c == "_":
            result.append(c)
        else:
            code = ord(c)
            result.append(f"%{code//16:X}{code%16:X}")

    return "".join(result)


def get_table_shared_id(ctx: Context, zk_path: str) -> str:
    """
    Tries to get table_shared_id from given path in zookeeper.
    """
    shared_id_path = os.path.join(zk_path, "table_shared_id")
    try:
        return get_zk_node(ctx, shared_id_path)
    except NoNodeError:
        return ""


@contextmanager
def zk_client(ctx: Context) -> Generator[KazooClient, None, None]:
    """
    Context manager for providing a started ZooKeeper client.

    Uses an existing client from the context if present,
    otherwise creates, starts, and injects a new one for the context.
    Cleans up (stops and removes) the client on exit if it was created here.
    """

    if "zk_client" in ctx.obj:
        yield ctx.obj["zk_client"]
    else:
        zk = _get_zk_client(ctx)
        zk.start()
        try:
            ctx.obj["zk_client"] = zk
            yield zk
        finally:
            del ctx.obj["zk_client"]
            zk.stop()


def _get_zk_client(ctx: Context) -> KazooClient:
    """
    Create and return KazooClient.
    """
    args = ctx.obj.get("zk_client_args", {})
    host = args.get("host")
    port = args.get("port", 2181)
    use_ssl = args.get("use_ssl", False)
    verify_ssl_certs = args.get("verify_ssl_certs", True)
    timeout = args.get("timeout", 10)
    zkcli_identity = args.get("zkcli_identity")
    no_chroot = args.get("no_chroot", False)
    no_ch_config = args.get("no_ch_config", False)
    zk_root_path = args.get("zk_root_path", None)
    zk_randomize_hosts = (
        ctx.obj["config"].get("zookeeper", {}).get("randomize_hosts", True)
    )

    if no_ch_config:
        if not host:
            host = "localhost"
        connect_str = f"{host}:{port}"
    else:
        # Intentionally don't try to load preprocessed.
        # We are not sure here if zookeeper-servers's changes already have been reloaded by CH.
        zk_config = get_clickhouse_config(ctx).zookeeper
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
        connect_str,
        auth_data=auth_data,
        timeout=timeout,
        logger=logging.getNativeLogger("kazoo"),
        use_ssl=use_ssl,
        verify_certs=verify_ssl_certs,
        randomize_hosts=zk_randomize_hosts,
    )
