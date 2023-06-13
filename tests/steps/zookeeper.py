"""
Steps for interacting with ZooKeeper or Clickhouse Keeper.
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
    zk_conf = context.conf['services']['clickhouse']['zookeeper']
    client = _zk_client(context, zk_conf["port"], zk_conf["secure"])
    try:
        client.start()
    finally:
        client.stop()


@given('a working keeper on {node:w}')
@retry(wait=wait_fixed(0.5), stop=stop_after_attempt(360))
def step_wait_for_keeper_alive(context, node):
    """
    Wait until clickhouse keeper is ready to accept incoming requests.
    """
    keeper_conf = context.conf['services']['clickhouse']['keeper']
    port = keeper_conf['port']
    use_ssl = keeper_conf['secure']

    client = _zk_client(context, port, use_ssl, node)
    try:
        client.start()
        client.get('/')
    except Exception:
        client.stop()
        raise
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

    zk_conf = context.conf['services']['clickhouse']['zookeeper']
    client = _zk_client(context, zk_conf["port"], zk_conf["secure"])

    try:
        client.start()
        recursive_remove_node_data(client, '/', node)
    finally:
        client.stop()


def _zk_client(context, port, use_ssl, instance_name='zookeeper01'):
    zk_container = get_container(context, instance_name)
    host, port = get_exposed_port(zk_container, port)

    return KazooClient(f'{host}:{port}', use_ssl=use_ssl)
