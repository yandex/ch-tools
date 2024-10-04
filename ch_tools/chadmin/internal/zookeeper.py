import ast
import os
import re
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from math import sqrt
from typing import Deque, Dict, Generator, List, Optional, Tuple

from click import BadParameter
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, NotEmptyError
from kazoo.interfaces import IAsyncResult

from ch_tools.chadmin.internal.database_replica import system_database_drop_replica
from ch_tools.chadmin.internal.system import match_ch_version
from ch_tools.chadmin.internal.table_replica import system_table_drop_replica
from ch_tools.chadmin.internal.utils import chunked, replace_macros
from ch_tools.common import logging
from ch_tools.common.clickhouse.config import get_clickhouse_config, get_macros
from ch_tools.common.process_pool import WorkerTask, execute_tasks_in_parallel

REPLICATED_DATABASE_MARKER = bytes("DatabaseReplicated", "utf-8")


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


def delete_zk_nodes(ctx, paths, dry_run=False):
    paths_formated = [_format_path(ctx, path) for path in paths]
    with zk_client(ctx) as zk:
        _delete_recursive(zk, paths_formated, dry_run)


def _format_path(ctx, path):
    args = ctx.obj.get("zk_client_args", {})
    no_ch_config = args.get("no_ch_config", False)
    if no_ch_config:
        return path
    return replace_macros(path, get_macros(ctx))


def _set_node_value(zk, path, value):
    """
    Set value to node in zk.
    """
    if zk.exists(path):
        try:
            zk.set(path, value.encode())
        except NoNodeError:
            logging.warning("Can not set for node: {}  value : {}", path, value)


def _find_paths(zk, root_path, included_paths_regexp, excluded_paths=None):
    """
    Traverse zookeeper tree from root_path with bfs approach.

    Return paths of nodes that match the include regular expression and do not match the excluded one.
    """
    paths = set()
    queue = deque([root_path])
    included_regexp = re.compile("|".join(included_paths_regexp))
    excluded_regexp = re.compile("|".join(excluded_paths)) if excluded_paths else None
    while len(queue):
        path = queue.popleft()
        if excluded_regexp and re.match(excluded_regexp, path):
            continue
        for child_node in _get_children(zk, path):
            subpath = os.path.join(path, child_node)

            if re.match(included_regexp, subpath):
                paths.add(subpath)
            else:
                queue.append(os.path.join(path, subpath))

    return list(paths)


def _delete_nodes_transaction(zk, to_delete_in_trasaction):
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


def _remove_subpaths(paths):
    """
    Removing from the list paths that are subpath of another.

    Example:
    [/a, /a/b/c<-remove it]
    """
    if not paths:
        return
    # Sorting the list in the lexicographic order
    paths.sort()
    paths = [path.split("/") for path in paths]
    normalized_paths = [paths[0]]
    # If path[i] has subnode path[j] then all paths from i to j will be subnode of i.
    for path in paths:
        last = normalized_paths[-1]
        # Ignore the path if the last normalized one is its prefix
        if len(last) > len(path) or path[: len(last)] != last:
            normalized_paths.append(path)
    return ["/".join(path) for path in normalized_paths]


def _delete_recursive(zk, paths, dry_run=False):
    """
    Kazoo already has the ability to recursively delete nodes, but the implementation is quite naive
    and has poor performance with a large number of nodes being deleted.

    In this implementation we unite the nodes to delete in transactions to do single operation for batch of nodes.
    To delete in correct order first of all we perform topological sort using bfs approach.
    """

    if len(paths) == 0:
        return

    logging.debug("Node to recursive delete {}", paths)
    paths = _remove_subpaths(paths)
    nodes_to_delete = []
    queue = deque(paths)

    while queue:
        path = queue.popleft()
        nodes_to_delete.append(path)
        for child_node in _get_children(zk, path):
            queue.append(os.path.join(path, child_node))

    logging.info("Got {} nodes to remove.", len(nodes_to_delete))
    if dry_run:
        return
    # When number of nodes to delete is large preferable to use greater transaction size.
    operations_in_transaction = max(100, int(sqrt(len(nodes_to_delete))))

    for transaction_orerations in chunked(
        reversed(nodes_to_delete), operations_in_transaction
    ):
        _delete_nodes_transaction(zk, transaction_orerations)


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


def replace_macros_in_nodes(func):
    def wrapper(ctx, nodes, *args, **kwargs):
        replace_macros_nodes = [_format_path(ctx, node) for node in nodes]
        return func(ctx, replace_macros_nodes, *args, **kwargs)

    return wrapper


@dataclass
class ZookeeperNode:
    path: str
    children: List[str]


class ZookeeperTreeInspector:
    """
    Class for traversal zookeeper tree.
    It is based on 3 queues (pending, active, finished). We are performing several (max_parrallel_tasks) async request,
    so active queue has limited size.
    Note: It doesn't inserts nodes, user should do it manually.

    There are two main methods:
        add() -> add list nodes to get children.
        get() -> get the one node with list of children.
    """

    def __init__(self, zk: KazooClient, max_parrallel_tasks: int = 5):
        self._zk_client = zk
        self._max_active_tasks = max_parrallel_tasks

        self._queue_pending: Deque[str] = deque()
        self._queue_active: Deque[Tuple[str, IAsyncResult]] = deque(
            maxlen=self._max_active_tasks
        )
        self._queue_finished: Deque[ZookeeperNode] = deque()

    def _update_pending_queue(self) -> None:
        """
        Moves tasks to active and creates async get_children requests.
        """

        number_tasks_to_move_to_active = min(
            len(self._queue_pending), self._max_active_tasks - len(self._queue_active)
        )

        for _ in range(0, number_tasks_to_move_to_active):
            path = self._queue_pending.popleft()
            async_task = self._zk_client.get_children_async(path)
            self._queue_active.append((path, async_task))

    def _update_active_queue(self, block_until_finised_tasks: bool = False) -> None:
        """
        Moves executed tasks from active to finished queues.
        """

        if block_until_finised_tasks:
            self._queue_active[0][1].wait()

        while len(self._queue_active) and self._queue_active[0][1].ready():
            async_task = self._queue_active.popleft()
            if async_task[1].successful():
                self._queue_finished.append(
                    ZookeeperNode(async_task[0], async_task[1].get())
                )
            else:
                if isinstance(async_task[1].exception, NoNodeError):
                    logging.warning(
                        "Ignoring node {}, because someone delete it", async_task[0]
                    )
                    continue
                raise async_task[1].exception

    def _update_queues(self, block_until_finished_tasks: bool = False) -> None:
        """
        Perform queues updates.
        """

        self._update_active_queue(block_until_finished_tasks)
        self._update_pending_queue()

    def _exists_tasks_to_do(self) -> bool:
        """
        Check that exists tasks in queues.
        """

        return (
            len(self._queue_finished)
            + len(self._queue_pending)
            + len(self._queue_active)
        ) > 0

    def _get_impl(self, with_block: bool = False) -> Optional[ZookeeperNode]:
        """
        Implementation of get functions. There are two mode:
        with_block - wait until one of the tasks finished.
        without_block - get one finished task if there are no finished, calls itself with block.
        """
        self._update_queues(with_block)

        if len(self._queue_finished):
            return self._queue_finished.popleft()

        if not self._exists_tasks_to_do():
            return None

        return self._get_impl(with_block=True)

    def add(self, pathes_to_inspect: List[str]) -> None:
        """
        Add list of paths in pending queue.
        """
        self._queue_pending.extend(pathes_to_inspect)
        self._update_queues()

    def get(self) -> Optional[ZookeeperNode]:
        """
        Get one finished task, if exists.
        """
        return self._get_impl(with_block=False)

    def tree_iterator(self) -> Generator[ZookeeperNode, None, None]:
        """
        Iterator for tree.
        """
        while True:
            next_zk_node = self.get()
            if not next_zk_node:
                return
            yield next_zk_node


# pylint: disable=too-many-statements
@replace_macros_in_nodes
def clean_zk_metadata_for_hosts(
    ctx,
    nodes,
    zk_cleanup_root_path="/",
    cleanup_tables=True,
    cleanup_database=True,
    cleanup_ddl_queue=True,
    zk_ddl_query_path=None,
    dry_run=False,
):
    """
    Perform cleanup in zookeeper after deleting hosts in the cluster or whole cluster deleting.
    """

    def _traverse_zk_tree_and_find_objects(
        zk: KazooClient,
        zk_root_path: str,
        excluded_paths: List[str],
        max_workers: int = 4,
    ) -> List[ZookeeperNode]:
        """
        Traverse zk tree and find replicated objects.
        """

        inspector = ZookeeperTreeInspector(zk, max_parrallel_tasks=max_workers)
        inspector.add([zk_root_path])

        objects_paths: List[ZookeeperNode] = []

        excluded_regexp = re.compile("|".join(excluded_paths))

        for zk_node in inspector.tree_iterator():
            if "replicas" in zk_node.children:
                objects_paths.append(zk_node)
                continue

            children_to_traverse: List[str] = []
            for children in zk_node.children:
                children_full_path = os.path.join(zk_node.path, children)
                if not re.match(excluded_regexp, children_full_path):
                    children_to_traverse.append(children_full_path)
            inspector.add(children_to_traverse)

        logging.info("Found {} replicated objects", len(objects_paths))
        return objects_paths

    def _collect_objects_for_cleanup(
        zk: KazooClient,
        zk_root_path: str,
        collect_tables: bool,
        collect_database: bool,
        max_workers: int = 4,
    ) -> Tuple[Dict[str, List[Tuple[str, str]]], Dict[str, List[str]]]:
        """
        Gets list of all replicated objects in zk, determine tables and databases for cleanup.
        """

        excluded_paths = [
            ".*clickhouse/task_queue",
            ".*clickhouse/zero_copy",
            ".*/blocks/.*",
            ".*/block_numbers/.*",
        ]

        replicated_objects: List[ZookeeperNode] = _traverse_zk_tree_and_find_objects(
            zk,
            zk_root_path,
            excluded_paths=excluded_paths,
            max_workers=max_workers,
        )

        nodes_set = set(nodes)

        databases_to_cleanup: Dict[str, List[Tuple[str, str]]] = {}
        tables_to_cleanup: Dict[str, List[str]] = {}

        for replicated_object in replicated_objects:
            # Actually rn in the ch (10.24) there are no secure way to determine that node is the root of replicated table.
            # So we are accuming that if object not database then it is table.
            #
            # Replicated table
            # https://github.com/ClickHouse/ClickHouse/blob/eb984db51ea0309dd607eaf98280315b42347f65/src/Interpreters/InterpreterSystemQuery.cpp#L1029
            #
            # Replicated Database
            # https://github.com/ClickHouse/ClickHouse/blob/eb984db51ea0309dd607eaf98280315b42347f65/src/Databases/DatabaseReplicated.cpp#L1529
            try:
                is_database = bool(
                    zk.get(replicated_object.path)[0] == REPLICATED_DATABASE_MARKER
                )
            except NoNodeError:
                logging.warning(
                    "Someone delete node {}, ignore it", replicated_object.path
                )
                continue

            is_table = not is_database
            if is_database and not collect_database:
                continue
            if is_table and not collect_tables:
                continue

            replicas_list = _get_children(
                zk, os.path.join(replicated_object.path, "replicas")
            )

            for replica in replicas_list:
                if is_database:
                    shard, replica_parsed = replica.split("|")
                    if replica_parsed in nodes_set:
                        if replicated_object.path not in databases_to_cleanup:
                            databases_to_cleanup[replicated_object.path] = []
                        databases_to_cleanup[replicated_object.path].append(
                            (shard, replica_parsed)
                        )
                elif is_table:
                    if replica in nodes_set:
                        if replicated_object.path not in tables_to_cleanup:
                            tables_to_cleanup[replicated_object.path] = []
                        tables_to_cleanup[replicated_object.path].append(replica)
                else:
                    raise RuntimeError("Unreacheble state")

        logging.info(
            "From {} objects, there are {} replicatad tables and {} replicated databases.",
            len(replicated_objects),
            len(tables_to_cleanup),
            len(databases_to_cleanup),
        )
        return (databases_to_cleanup, tables_to_cleanup)

    def cleanup_tables_and_databases(zk: KazooClient) -> None:
        """
        Collects all objects that have nodes to be deleted and runs drop repliaca for them.
        """
        max_workers = ctx.obj["config"]["chadmin"]["zookeeper"][
            "clean_zk_metadata_for_hosts"
        ]["workers"]

        database_to_drop, tables_to_drop = _collect_objects_for_cleanup(
            zk,
            zk_root_path,
            collect_database=cleanup_database,
            collect_tables=cleanup_tables,
            max_workers=max_workers,
        )

        tasks: List[WorkerTask] = []
        if cleanup_tables:
            for zk_table_path, list_of_nodes in tables_to_drop.items():
                for node in list_of_nodes:
                    tasks.append(
                        WorkerTask(
                            f"system_table_drop_replica_{node}",
                            system_table_drop_replica,
                            {
                                "ctx": ctx,
                                "table_zk_path": zk_table_path,
                                "replica": node,
                                "dry_run": dry_run,
                            },
                        )
                    )

        if cleanup_database:
            if match_ch_version(ctx, min_version="23.1"):
                for zk_database_path, list_of_nodes in database_to_drop.items():  # type: ignore
                    for node in list_of_nodes:
                        replica = f"{node[0]}|{node[1]}"
                        tasks.append(
                            WorkerTask(
                                f"system_database_drop_replica_{replica}",
                                system_database_drop_replica,
                                {
                                    "ctx": ctx,
                                    "database_zk_path": zk_database_path,
                                    "replica": replica,
                                    "dry_run": dry_run,
                                },
                            )
                        )
            else:
                logging.warning(
                    "Ch version is too old, will skip replicated database cleanup."
                )

        execute_tasks_in_parallel(tasks, max_workers=max_workers)

    def _get_hosts_from_ddl(zk, ddl_task_path):
        """
        Extract hostnames from the ddl task. Returns in dict format as { 'escaped_hostname'  : 'escaped_hostname:port' }
        Example of ddl:

        version: 5
        query: CREATE TABLE default.test ...
        hosts: ['host:port', ...]
        initiator: host:port
        settings: s3_min_upload_part_size = 33554432, s3_max_single_part_upload_size = 33554432, distributed_foreground_insert = true, distributed_background_insert_batch = true, log_queries = false, log_queries_cut_to_length = 10000000, max_concurrent_queries_for_user = 450, ignore_on_cluster_for_replicated_udf_queries = true, ignore_on_cluster_for_replicated_access_entities_queries = true, timeout_before_checking_execution_speed = 300., join_algorithm = 'auto,direct', allow_drop_detached = true, database_atomic_wait_for_drop_and_detach_synchronously = true, kafka_disable_num_consumers_limit = true, force_remove_data_recursively_on_drop = true
        tracing: 00000000-0000-0000-0000-000000000000
        """
        try:
            for line in zk.get(ddl_task_path)[0].decode().strip().splitlines():
                if "hosts: " in line:
                    host_names_with_ports = ast.literal_eval(line[len("hosts: ") :])
                    result = {
                        hosts_with_port[: hosts_with_port.find(":")]: hosts_with_port
                        for hosts_with_port in host_names_with_ports
                    }
                    return result
        except NoNodeError:
            pass

    def _is_ddl_task_finished(zk, ddl_number_of_hosts, ddl_path):
        """
        Check that ddl is finished.
        """
        if not zk.exists(ddl_path):
            return True

        return ddl_number_of_hosts == len(
            _get_children(zk, os.path.join(ddl_path, "finished/"))
        )

    def _wait_ddl_tasks_finished(
        zk, dll_tasks_to_remove, wait_time=0.3, max_attemps=20
    ):
        """
        Go thought list of tasks to remove and tries to wait until all of them finish.
        """
        for _ in range(0, max_attemps):
            for ddl_data in dll_tasks_to_remove:
                ddl_path = ddl_data[0]
                ddl_size = ddl_data[1]
                if not _is_ddl_task_finished(zk, ddl_size, ddl_path):
                    break
            else:
                return
            time.sleep(wait_time)

        # Even if ddl task not finished, we must remove it. So just warn it.
        for ddl_data in dll_tasks_to_remove:
            if not _is_ddl_task_finished(zk, ddl_data[1], ddl_data[0]):
                logging.warning(
                    "DDL task {} is not finished. Will remove it anyway.",
                    ddl_data[0],
                )

    def mark_finished_ddl_query(zk):
        """
        If after deleting a host there are still unfinished ddl tasks in the queue,
        then we pretend that the host has completed this task.

        """
        ddl_tasks_to_remove = []

        logging.info("Start ddl query cleanup for nodes: {}", ",".join(nodes))

        for ddl_task in _get_children(zk, _format_path(ctx, zk_ddl_query_path)):
            ddl_task_full = os.path.join(zk_ddl_query_path, ddl_task)
            logging.info("DDL task full path: {}", ddl_task_full)

            have_mention = False
            ddl_hosts_dict = _get_hosts_from_ddl(zk, ddl_task_full)

            for host in nodes:
                escaped_host_name = escape_for_zookeeper(host)
                if escaped_host_name not in ddl_hosts_dict:
                    logging.info("Host is not mentioned in DDL task value")
                    continue

                have_mention = True
                finished_path = os.path.join(
                    ddl_task_full, f"finished/{ddl_hosts_dict[escaped_host_name]}"
                )

                if zk.exists(finished_path):
                    logging.info("Finished path already exists: {}", finished_path)
                    continue
                logging.info("Add {} to finished for ddl_task: {}", host, ddl_task)
                if not dry_run:
                    zk.create(finished_path, b"0\n")

            if have_mention:
                ddl_tasks_to_remove.append([ddl_task_full, len(ddl_hosts_dict)])

        logging.info("Finish mark ddl query")
        chunk_size = 100
        for ddl_task_chunk in chunked(ddl_tasks_to_remove, chunk_size):
            if not dry_run:
                _wait_ddl_tasks_finished(zk, ddl_task_chunk)
            delete_zk_nodes(ctx, [ddl_data[0] for ddl_data in ddl_task_chunk], dry_run)

        logging.info("DDL queue cleanup finished")

    zk_root_path = _format_path(ctx, zk_cleanup_root_path)

    with zk_client(ctx) as zk:

        cleanup_tables_and_databases(zk)

        if cleanup_ddl_queue:
            if not zk_ddl_query_path:
                raise BadParameter(
                    "Trying to clean ddl queue, but the ddl queue path is not specified."
                )

            mark_finished_ddl_query(zk)


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
