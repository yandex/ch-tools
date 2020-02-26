"""
Steps for interacting with ZooKeeper.
"""
from behave import given
from kazoo.client import KazooClient
from tenacity import retry, stop_after_attempt, wait_fixed

from modules.docker import get_container, get_exposed_port


@given('a working zookeeper')
@retry(wait=wait_fixed(0.5), stop=stop_after_attempt(360))
def step_wait_for_zookeeper_alive(context):
    zk_container = get_container(context, 'zookeeper01')
    host, port = get_exposed_port(zk_container, 2181)

    client = KazooClient(f'{host}:{port}')
    try:
        client.start()
    finally:
        client.stop()
