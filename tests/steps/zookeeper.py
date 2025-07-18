"""
Steps for interacting with ZooKeeper or Clickhouse Keeper.
"""

import os
from typing import Any

from behave import given, then
from click import Context
from kazoo.client import KazooClient
from modules.docker import get_container, get_exposed_port
from tenacity import retry, stop_after_attempt, wait_fixed

from ch_tools.common import logging


@given("a working zookeeper")
@retry(wait=wait_fixed(0.5), stop=stop_after_attempt(40))
def step_wait_for_zookeeper_alive(context: Context) -> None:
    """
    Ensure that ZK is ready to accept incoming requests.
    """
    client = _zk_client(context)
    try:
        client.start()
    finally:
        client.stop()


@given("a working keeper on {node:w}")
@retry(wait=wait_fixed(0.5), stop=stop_after_attempt(20))
def step_wait_for_keeper_alive(context: Context, node: Any) -> None:
    """
    Wait until clickhouse keeper is ready to accept incoming requests.
    """
    client = _zk_client(context, instance_name=node, port=2281, use_ssl=True)
    try:
        client.start()
        client.get("/")
    except Exception:
        client.stop()
        raise
    finally:
        client.stop()


@given("we have removed ZK metadata for {node:w}")
def clean_zk_tables_metadata_for_host(context: Context, node: Any) -> None:
    """
    Remove all metadata for specified host from ZK
    """

    def recursive_remove_node_data(zk_client: Any, path: Any, node: Any) -> None:
        for subpath in zk_client.get_children(path):
            if subpath == node:
                zk_client.delete(os.path.join(path, subpath), recursive=True)
            else:
                recursive_remove_node_data(zk_client, os.path.join(path, subpath), node)

    client = _zk_client(context)

    try:
        client.start()
        recursive_remove_node_data(client, "/", node)
    finally:
        client.stop()


@then('we get zookeeper node with "{path}" path')
def step_get_zk_node(context: Context, path: Any) -> None:
    client = _zk_client(context)

    try:
        client.start()
        result = client.get(path)[0].decode().strip()
    finally:
        client.stop()

    print(result)


def _zk_client(
    context: Context,
    instance_name: str = "zookeeper01",
    port: int = 2181,
    use_ssl: bool = False,
) -> Any:
    logging.set_module_log_level("kazoo", logging.CRITICAL)

    zk_container = get_container(context, instance_name)
    host, port = get_exposed_port(zk_container, port)

    return KazooClient(f"{host}:{port}", use_ssl=use_ssl, verify_certs=False)
