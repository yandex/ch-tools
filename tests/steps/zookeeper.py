"""
Steps for interacting with ZooKeeper.
"""
import os

from behave import given, then, when
from hamcrest import assert_that, has_length
from kazoo.client import KazooClient
from tenacity import retry, stop_after_attempt, wait_fixed

from modules.docker import get_container, get_exposed_port


@given('a working zookeeper')
@retry(wait=wait_fixed(0.5), stop=stop_after_attempt(360))
def step_wait_for_zookeeper_alive(context):
    """
    Ensure that ZK is ready to accept incoming requests.
    """
    client = _zk_client(context)
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
    client = _zk_client(context, instance_name=node, port=context.conf['services']['clickhouse']['keeper']['port'])
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

    client = _zk_client(context)
    try:
        client.start()
        recursive_remove_node_data(client, '/', node)
    finally:
        client.stop()


@when('we execute ZK list query on {node:w}')
def step_zk_list_query(context, node):
    zk_client = _zk_client(context, node)
    try:
        zk_client.start()
        result = zk_client.get_children(context.text)
        context.response = ';'.join(user for user in result)
    finally:
        zk_client.stop()


@then('we get ZK list with len {length:d}')
def step_zk_list_len(context, length):
    response = context.response if isinstance(context.response, list) else context.response.split(';')
    assert_that(response, has_length(length))


def _zk_client(context, instance_name='zookeeper01', port=2181):
    zk_container = get_container(context, instance_name)
    host, port = get_exposed_port(zk_container, port)

    return KazooClient(f'{host}:{port}')
