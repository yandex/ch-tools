"""
Steps for interacting with ZooKeeper.
"""
import os

from behave import given
from kazoo.client import KazooClient
from tenacity import retry, stop_after_attempt, wait_fixed

from modules.docker import get_container, get_exposed_port


@given('a working zookeeper')
@retry(wait=wait_fixed(0.5), stop=stop_after_attempt(360))
def step_wait_for_zookeeper_alive(context):
    """
    Ensure that ZK is ready to accept incoming requests.
    """
    zk_container = get_container(context, 'zookeeper01')
    host, port = get_exposed_port(zk_container, 2181)

    client = KazooClient(f'{host}:{port}')
    try:
        client.start()
    finally:
        client.stop()


@given('we have removed ZK metadata for {node:w}')
def clean_zk_tables_metadata_for_host(context, node):
    """
    Remove all metadata for specified host from ZK
    """
    def recursive_remove_node_data(zk_client, path, node):
        for subpath in zk_client.get_children(path):
            if subpath == node:
                zk_client.delete(os.path.join(path, subpath), recursive=True)
            else:
                recursive_remove_node_data(zk_client, os.path.join(path, subpath), node)

    zk_container = get_container(context, 'zookeeper01')
    host, port = get_exposed_port(zk_container, 2181)
    client = KazooClient(f'{host}:{port}')

    try:
        client.start()
        recursive_remove_node_data(client, '/', node)
    finally:
        client.stop()
