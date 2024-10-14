import ast
import os
import re
import time
from collections import deque
from contextlib import contextmanager
from math import sqrt
from typing import List

from click import BadParameter
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, NotEmptyError

from ch_tools.chadmin.internal.utils import chunked, replace_macros
from ch_tools.common import logging
from ch_tools.common.clickhouse.config import get_clickhouse_config, get_macros


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

    def _find_tables(zk, root_path, nodes):
        """
        Find nodes mentions in zk for table management.
        """
        excluded_paths = [
            ".*clickhouse/task_queue",
            ".*clickhouse/zero_copy",
            ".*/blocks/.*",
            ".*/block_numbers/.*",
        ]

        hosts_mentions = _find_paths(
            zk, root_path, [".*/replicas/" + node for node in nodes], excluded_paths
        )

        # Paths will be like */shard1/replicas/hostname. But we need */shard1.
        # Go up for 2 directories.
        table_paths = [os.sep.join(path.split(os.sep)[:-2]) for path in hosts_mentions]
        # One path might be in list several times. Make list unique.
        return (list(set(table_paths)), hosts_mentions)

    def _find_databases(zk, root_path, nodes):
        """
        Databases contains replicas in zk in cases when engine is Replicated with
        pattern: <shard>|<replica>
        """
        excluded_paths = [
            ".*clickhouse/task_queue",
            ".*clickhouse/zero_copy",
            ".*/blocks/.*",
            ".*/block_numbers/.*",
        ]
        hosts_mentions = _find_paths(
            zk,
            root_path,
            [".*/replicas/.*[|]" + node for node in nodes],
            excluded_paths,
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
                logging.info("Set is_lost_flag {}", is_lost_flag_path)
                if not dry_run:
                    _set_node_value(zk, is_lost_flag_path, "1")

    def _get_replicas_queues_to_remove(zk, paths, replica_names):
        """
        Remove <path>/replicas/<replica_name>/queue
        """
        to_remove: List[str] = []
        for path in paths:
            replica_name = os.path.join(path, "replicas")
            if not zk.exists(replica_name):
                continue

            for replica_name in replica_names:
                queue_path = os.path.join(replica_name, replica_name, "queue")
                if not zk.exists(queue_path):
                    continue

                if zk.exists(queue_path):
                    to_remove.append(queue_path)
        return to_remove

    def _remove_if_no_hosts_in_replicas(zk, paths):
        """
        Remove node if subnode is empty
        """
        to_delete = []
        for path in paths:
            child_full_path = os.path.join(path, "replicas")
            if (
                not zk.exists(child_full_path)
                or len(_get_children(zk, child_full_path)) == 0
            ):
                to_delete.append(path)
        _delete_recursive(zk, to_delete, dry_run)

    def tables_cleanup(zk, zk_root_path):
        """
        Do cleanup for tables which contains nodes.
        """
        logging.info("Start tables cleanup for nodes: {}", ",".join(nodes))
        (table_paths, hosts_paths) = _find_tables(zk, zk_root_path, nodes)
        _set_replicas_is_lost(zk, table_paths, nodes)

        to_remove = _get_replicas_queues_to_remove(zk, table_paths, nodes) + hosts_paths
        _delete_recursive(zk, to_remove, dry_run)
        _remove_if_no_hosts_in_replicas(zk, table_paths)

    def databases_cleanups(zk, zk_root_path):
        """
        Do cleanup for databases which contains nodes.
        """
        logging.info("Start databases cleanup for nodes: {}", ",".join(nodes))
        (databases_paths, hosts_paths) = _find_databases(zk, zk_root_path, nodes)
        _delete_recursive(zk, hosts_paths, dry_run)
        _remove_if_no_hosts_in_replicas(zk, databases_paths)

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
        if cleanup_tables:
            tables_cleanup(zk, zk_root_path)

        if cleanup_database:
            databases_cleanups(zk, zk_root_path)

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
