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
from dataclasses import dataclass, field
from time import monotonic
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Set, Union

from click import Context
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError, NotEmptyError
from kazoo.protocol.states import ZnodeStat
from kazoo.retry import KazooRetry

from ch_tools.chadmin.internal.utils import replace_macros
from ch_tools.common import logging
from ch_tools.common.clickhouse.config import get_clickhouse_config, get_macros
from ch_tools.common.clickhouse.config.clickhouse import ClickhouseConfig
from ch_tools.common.utils import escape_for_file_name, unescape_for_file_name

LARGE_RECURSIVE_DELETE_LOG_INTERVAL = 100_000
RECURSIVE_DELETE_TRANSACTION_MAX_OPS = 1_000
RECURSIVE_DELETE_TRANSACTION_MAX_BYTES = 512 * 1024
RECURSIVE_DELETE_OPERATION_OVERHEAD = 32
RECURSIVE_DELETE_DRY_RUN_MAX_LISTED_PATHS = 1_000


@dataclass
class _DeleteCounts:
    deleted: int = 0
    already_absent: int = 0
    not_empty: int = 0


@dataclass
class _DeleteProgress:
    counts: _DeleteCounts = field(default_factory=_DeleteCounts)
    sweeps: int = 0
    retained_branches: int = 0


@dataclass
class _ProbeResult:
    postorder: Optional[List[str]]
    deadline_reached: bool = False


def _path_delete_size(path: str) -> int:
    return len(path.encode("utf-8")) + RECURSIVE_DELETE_OPERATION_OVERHEAD


def _probe_subtree(
    zk: KazooClient, root_path: str, deadline: Optional[float] = None
) -> _ProbeResult:
    estimated_bytes = _path_delete_size(root_path)
    if estimated_bytes > RECURSIVE_DELETE_TRANSACTION_MAX_BYTES:
        return _ProbeResult(None)

    operation_count = 1
    preorder = []
    pending = [root_path]

    while pending:
        if deadline is not None and monotonic() >= deadline:
            return _ProbeResult(None, deadline_reached=True)
        path = pending.pop()
        preorder.append(path)
        children = get_children(zk, path)
        child_paths = []
        for child in children:
            child_path = os.path.join(path, child)
            operation_count += 1
            estimated_bytes += _path_delete_size(child_path)
            if (
                operation_count > RECURSIVE_DELETE_TRANSACTION_MAX_OPS
                or estimated_bytes > RECURSIVE_DELETE_TRANSACTION_MAX_BYTES
            ):
                return _ProbeResult(None)
            child_paths.append(child_path)
        pending.extend(reversed(child_paths))

    return _ProbeResult(list(reversed(preorder)))


def _delete_candidates(
    zk: KazooClient, paths: List[str], counts: _DeleteCounts
) -> List[int]:
    """Update counts and return input indices of nonempty nodes."""
    if not paths:
        return []

    estimated_bytes = sum(_path_delete_size(path) for path in paths)
    if (
        len(paths) <= RECURSIVE_DELETE_TRANSACTION_MAX_OPS
        and estimated_bytes <= RECURSIVE_DELETE_TRANSACTION_MAX_BYTES
    ):
        transaction = zk.transaction()
        for path in paths:
            transaction.delete(path)
        result = transaction.commit()
        if len(result) == len(paths) and all(item is True for item in result):
            counts.deleted += len(paths)
            return []

        logging.info(
            "Delete transaction failed; falling back to {} individual deletes",
            len(paths),
        )

    not_empty_indices = []
    for index, path in enumerate(paths):
        try:
            zk.delete(path)
            counts.deleted += 1
        except NoNodeError:
            counts.already_absent += 1
        except NotEmptyError:
            counts.not_empty += 1
            not_empty_indices.append(index)
    return not_empty_indices


class ZKTransactionBuilder:
    """
    Builder for ZooKeeper transactions with path tracking and automatic validation.
    Supports context manager protocol for creating/deleting nodes atomically.
    """

    def __init__(self, ctx: Context, zk: KazooClient) -> None:
        self.ctx = ctx
        self.zk = zk
        self.txn = zk.transaction()
        self.path_to_nodes: List[str] = []
        self._committed = False
        self._reset_called = False

    def __enter__(self) -> "ZKTransactionBuilder":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        # Only auto-commit if no exception and neither commit() nor reset() was called
        if exc_type is None and not self._committed and not self._reset_called:
            self.commit()
        self.reset()

    def create_node(self, path: str, value: str = "") -> "ZKTransactionBuilder":
        if self._committed:
            raise RuntimeError("Cannot add operations to committed transaction")
        self.path_to_nodes.append(path)
        self.txn.create(path=format_path(self.ctx, path), value=value.encode())
        return self

    def delete_node(self, path: str) -> "ZKTransactionBuilder":
        if self._committed:
            raise RuntimeError("Cannot add operations to committed transaction")
        self.path_to_nodes.append(path)
        self.txn.delete(path=format_path(self.ctx, path))
        return self

    def commit(self) -> None:
        if self._committed:
            raise RuntimeError("Transaction already committed")

        result = self.txn.commit()

        # Check results and handle errors
        self._check_result_txn(result)

        self._committed = True

    def reset(self) -> None:
        self.path_to_nodes = []
        self.txn = self.zk.transaction()
        self._committed = False
        self._reset_called = True

    def _check_result_txn(self, results: List) -> None:
        """
        Validate transaction results, log all statuses if there's an error, and raise the first exception.
        """
        first_exception = None
        status_messages = []

        # Single pass: collect all status messages and capture first exception
        for path, result in zip(self.path_to_nodes, results):
            status_messages.append((path, result))
            # Check if result is an exception (indicating an error)
            # Successful operations return True (for delete) or a string path (for create)
            if isinstance(result, BaseException) and first_exception is None:
                first_exception = result

        # If there were errors, log all statuses and raise the first exception
        if first_exception is not None:
            for status in status_messages:
                logging.error(f"{status}")
            logging.error(
                f"Transaction error: {first_exception}, type={type(first_exception)}"
            )
            raise first_exception


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


def delete_zk_nodes(
    ctx: Context,
    paths: List[str],
    dry_run: bool = False,
    max_sweeps: int = 3,
    delete_timeout: Optional[float] = None,
) -> None:
    paths_formated = [format_path(ctx, path) for path in paths]
    with zk_client(ctx) as zk:
        delete_recursive(
            zk,
            paths_formated,
            dry_run,
            max_sweeps=max_sweeps,
            delete_timeout=delete_timeout,
        )


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


@dataclass
class _DeleteFrame:
    """Discovery state for one node; pending work belongs to one sibling batch."""

    path: str
    children: Optional[List[str]] = None
    next_child: int = 0
    # Children that rejected deletion and still need their descendants visited.
    to_expand: List[str] = field(default_factory=list)
    # Visited children awaiting another deletion attempt, grouped at their parent.
    ready: List[str] = field(default_factory=list)
    retained: bool = False


def _walk_subtree_depth_first(
    zk: KazooClient, root_path: str
) -> Generator[str, None, None]:
    """Yield paths without retaining every full path in a wide subtree."""
    yield root_path
    stack = [(root_path, get_children(zk, root_path), 0)]

    while stack:
        parent, children, index = stack[-1]
        if index == len(children):
            stack.pop()
            continue

        child_path = os.path.join(parent, children[index])
        stack[-1] = (parent, children, index + 1)
        yield child_path
        child_nodes = get_children(zk, child_path)
        if child_nodes:
            stack.append((child_path, child_nodes, 0))


def _take_child_batch(
    parent: str, children: List[str], start: int
) -> tuple[List[str], int]:
    """Build a sibling batch and return the next unread child index.

    A path exceeding the byte limit is emitted alone so traversal can advance;
    _delete_candidates handles it with an individual delete instead of multi.
    """
    batch: List[str] = []
    estimated_bytes = 0
    index = start
    while index < len(children) and len(batch) < RECURSIVE_DELETE_TRANSACTION_MAX_OPS:
        path = os.path.join(parent, children[index])
        path_size = _path_delete_size(path)
        if (
            batch
            and estimated_bytes + path_size > RECURSIVE_DELETE_TRANSACTION_MAX_BYTES
        ):
            break
        batch.append(path)
        estimated_bytes += path_size
        index += 1
        if estimated_bytes > RECURSIVE_DELETE_TRANSACTION_MAX_BYTES:
            break
    return batch, index


def _delete_candidates_with_progress_log(
    zk: KazooClient,
    root_path: str,
    paths: List[str],
    progress: _DeleteProgress,
) -> List[int]:
    deleted_before = progress.counts.deleted
    not_empty_indices = _delete_candidates(zk, paths, progress.counts)
    if (
        progress.counts.deleted // LARGE_RECURSIVE_DELETE_LOG_INTERVAL
        > deleted_before // LARGE_RECURSIVE_DELETE_LOG_INTERVAL
    ):
        logging.info(
            "Large recursive ZooKeeper deletion of {} is in progress: "
            "deleted={}, already_absent={}, not_empty={}",
            root_path,
            progress.counts.deleted,
            progress.counts.already_absent,
            progress.counts.not_empty,
        )
    return not_empty_indices


def _delete_sweep(
    zk: KazooClient,
    root_path: str,
    progress: _DeleteProgress,
    deadline: Optional[float] = None,
) -> tuple[int, bool]:
    """Run one depth-first sweep; return retained branch count and deadline state.

    Count final NOT_EMPTY results, including the root, rather than all surviving
    nodes or skipped ancestors. Update progress for every completed deletion.
    """
    stack = [_DeleteFrame(root_path)]
    retained_branches = 0

    while stack:
        if deadline is not None and monotonic() >= deadline:
            return retained_branches, True

        frame = stack[-1]

        if frame.children is None:
            # ZooKeeper returns the complete child list; batch limits cannot
            # bound this allocation.
            frame.children = get_children(zk, frame.path)
            continue

        if frame.to_expand:
            stack.append(_DeleteFrame(frame.to_expand.pop()))
            continue

        if frame.ready:
            # Writers may have added children since discovery. Defer another
            # discovery to the next sweep so this retry cannot loop forever.
            not_empty_indices = _delete_candidates_with_progress_log(
                zk, root_path, frame.ready, progress
            )
            if not_empty_indices:
                frame.retained = True
            retained_branches += len(not_empty_indices)
            frame.ready = []
            continue

        if frame.next_child < len(frame.children):
            # Optimistically delete leaves without a get_children call per leaf.
            batch, frame.next_child = _take_child_batch(
                frame.path, frame.children, frame.next_child
            )
            not_empty_indices = _delete_candidates_with_progress_log(
                zk, root_path, batch, progress
            )
            frame.to_expand.extend(batch[index] for index in not_empty_indices)
            continue

        stack.pop()
        if frame.retained:
            # A surviving descendant prevents deletion of this node and all
            # its ancestors. Propagate that fact, not a skip of sibling work.
            if stack:
                stack[-1].retained = True
            continue

        if stack:
            stack[-1].ready.append(frame.path)
            continue

        not_empty_indices = _delete_candidates_with_progress_log(
            zk, root_path, [frame.path], progress
        )
        retained_branches += len(not_empty_indices)

    return retained_branches, False


def _delete_atomic(
    zk: KazooClient, paths: List[str], progress: _DeleteProgress
) -> bool:
    transaction = zk.transaction()
    for path in paths:
        transaction.delete(path)
    result = transaction.commit()
    if len(result) != len(paths) or not all(item is True for item in result):
        return False
    progress.counts.deleted += len(paths)
    return True


def _dry_run_delete_recursive(zk: KazooClient, paths: List[str]) -> None:
    node_count = 0
    dry_run_nodes: Optional[List[str]] = []
    for path in paths:
        for node in _walk_subtree_depth_first(zk, path):
            node_count += 1
            if dry_run_nodes is not None:
                if len(dry_run_nodes) < RECURSIVE_DELETE_DRY_RUN_MAX_LISTED_PATHS:
                    dry_run_nodes.append(node)
                else:
                    dry_run_nodes = None
    logging.info("Got {} nodes to remove.", node_count)
    if dry_run_nodes is None:
        logging.info(
            "Would delete {} nodes; path list omitted because it exceeds {} entries",
            node_count,
            RECURSIVE_DELETE_DRY_RUN_MAX_LISTED_PATHS,
        )
    else:
        logging.info("Would delete nodes: {}", dry_run_nodes)


def delete_recursive(
    zk: KazooClient,
    paths: List[str],
    dry_run: bool = False,
    max_sweeps: int = 3,
    delete_timeout: Optional[float] = None,
) -> None:
    """
    Delete complete ZooKeeper subtrees.

    Small observed trees use one children-first atomic transaction. Large trees use
    bounded sibling batches and discover only candidates reported as nonempty.
    Concurrently changed branches are retained for a later finite sweep.
    """
    # pylint: disable=too-many-branches

    if len(paths) == 0:
        return
    if max_sweeps < 0:
        raise ValueError("max_sweeps must be non-negative")
    if delete_timeout is not None and delete_timeout <= 0:
        raise ValueError("delete_timeout must be positive")

    deadline = monotonic() + delete_timeout if delete_timeout is not None else None

    logging.debug("Node to recursive delete {}", paths)
    paths = remove_subpaths(paths)
    if dry_run:
        _dry_run_delete_recursive(zk, paths)
        return

    for root_path in paths:
        root_stat = zk.exists(root_path)
        if root_stat is None:
            logging.info(
                "Recursive ZooKeeper deletion of {} completed: root already absent",
                root_path,
            )
            continue

        progress = _DeleteProgress()
        try:
            if root_stat.children_count >= RECURSIVE_DELETE_TRANSACTION_MAX_OPS:
                probe = _ProbeResult(None)
            else:
                probe = _probe_subtree(zk, root_path, deadline)
            if probe.deadline_reached:
                raise RuntimeError(
                    f"Recursive deletion of {root_path} is incomplete: "
                    "delete deadline reached"
                )
            if deadline is not None and monotonic() >= deadline:
                raise RuntimeError(
                    f"Recursive deletion of {root_path} is incomplete: "
                    "delete deadline reached"
                )
            if probe.postorder is not None:
                logging.info(
                    "Using atomic recursive ZooKeeper deletion for {}: nodes={}",
                    root_path,
                    len(probe.postorder),
                )
                if _delete_atomic(zk, probe.postorder, progress):
                    if zk.exists(root_path) is not None:
                        raise RuntimeError(
                            f"Recursive deletion of {root_path} is incomplete: "
                            "root still exists"
                        )
                    logging.info(
                        "Recursive ZooKeeper deletion of {} completed: "
                        "deleted={}, already_absent={}",
                        root_path,
                        progress.counts.deleted,
                        progress.counts.already_absent,
                    )
                    continue
                logging.info(
                    "Atomic recursive ZooKeeper deletion of {} failed; "
                    "switching to large mode",
                    root_path,
                )

            logging.info(
                "Using large recursive ZooKeeper deletion for {}: "
                "transaction_max_ops={}, transaction_max_bytes={}, max_sweeps={}",
                root_path,
                RECURSIVE_DELETE_TRANSACTION_MAX_OPS,
                RECURSIVE_DELETE_TRANSACTION_MAX_BYTES,
                max_sweeps,
            )

            sweeps = 0
            while True:
                sweeps += 1
                retained, deadline_reached = _delete_sweep(
                    zk, root_path, progress, deadline
                )
                progress.sweeps = sweeps
                progress.retained_branches = retained
                root_exists = zk.exists(root_path) is not None
                logging.info(
                    "Recursive ZooKeeper deletion sweep {} for {} completed: "
                    "deleted={}, already_absent={}, not_empty={}, retained={}",
                    sweeps,
                    root_path,
                    progress.counts.deleted,
                    progress.counts.already_absent,
                    progress.counts.not_empty,
                    retained,
                )
                if not root_exists:
                    break
                if deadline_reached or (
                    deadline is not None and monotonic() >= deadline
                ):
                    raise RuntimeError(
                        f"Recursive deletion of {root_path} is incomplete: "
                        "delete deadline reached"
                    )
                if max_sweeps and sweeps >= max_sweeps:
                    raise RuntimeError(
                        f"Recursive deletion of {root_path} is incomplete after "
                        f"{sweeps} sweeps: root still exists"
                    )
        except Exception:
            logging.error(
                "Recursive ZooKeeper deletion of {} is incomplete: "
                "deleted={}, already_absent={}, not_empty={}",
                root_path,
                progress.counts.deleted,
                progress.counts.already_absent,
                progress.counts.not_empty,
            )
            raise

        logging.info(
            "Recursive ZooKeeper deletion of {} completed: "
            "deleted={}, already_absent={}, not_empty={}, sweeps={}",
            root_path,
            progress.counts.deleted,
            progress.counts.already_absent,
            progress.counts.not_empty,
            sweeps,
        )


def escape_for_zookeeper(s: str) -> str:
    """
    Escape string for ZooKeeper node names using ClickHouse's escapeForFileName logic.

    Alphanumeric characters and underscores are kept as-is.
    Other characters are encoded as %XX where XX is the hexadecimal character code.

    Example: "table-name" -> "table%2Dname"
    """
    return escape_for_file_name(s)


def unescape_from_zookeeper(s: str) -> str:
    """
    Unescape string from ZooKeeper node names.

    Decodes %XX sequences back to their original characters.
    Matches ClickHouse's unescapeForFileName logic.

    Example: "table%2Dname" -> "table-name"
    """
    return unescape_for_file_name(s)


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
    zk_config_section = ctx.obj["config"].get("zookeeper", {})
    zk_randomize_hosts = zk_config_section.get("randomize_hosts", True)
    zk_username = zk_config_section.get("username")
    zk_password = zk_config_section.get("password")

    # Only create KazooRetry when config is explicitly provided
    # This preserves KazooClient's default behavior when not specified
    connection_retry = (
        KazooRetry(**zk_config_section["connection_retry"])
        if "connection_retry" in zk_config_section
        else None
    )
    command_retry = (
        KazooRetry(**zk_config_section["command_retry"])
        if "command_retry" in zk_config_section
        else None
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
    elif zk_username and zk_password:
        auth_data = [("digest", f"{zk_username}:{zk_password}")]

    return KazooClient(
        connect_str,
        auth_data=auth_data,
        timeout=timeout,
        connection_retry=connection_retry,
        command_retry=command_retry,
        logger=logging.getNativeLogger("kazoo"),
        use_ssl=use_ssl,
        verify_certs=verify_ssl_certs,
        randomize_hosts=zk_randomize_hosts,
    )
