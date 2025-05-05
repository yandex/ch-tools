"""
Steps for interacting with chadmin.
"""

from behave import then, when
from hamcrest import assert_that, equal_to
from modules.chadmin import Chadmin
from modules.docker import get_container
from modules.typing import ContextT


@when("we execute chadmin create zk nodes on {node:w}")
def step_create_(context: ContextT, node: str) -> None:
    container = get_container(context, node)
    nodes = context.text.strip().split("\n")
    chadmin = Chadmin(container)

    for node in nodes:
        result = chadmin.create_zk_node(node)
        assert result.exit_code == 0, f" output:\n {result.output.decode().strip()}"


@when("we do hosts cleanup on {node} with fqdn {fqdn} and zk root {zk_root}")
def step_host_cleanup_with_zk_root(
    context: ContextT, node: str, fqdn: str, zk_root: str
) -> None:
    container = get_container(context, node)
    result = Chadmin(container).zk_cleanup(fqdn, zk_root)
    assert result.exit_code == 0, f" output:\n {result.output.decode().strip()}"


@when("we do hosts dry cleanup on {node} with fqdn {fqdn} and zk root {zk_root}")
def step_host_dry_cleanup_with_zk_root(
    context: ContextT, node: str, fqdn: str, zk_root: str
) -> None:
    container = get_container(context, node)
    result = Chadmin(container).zk_cleanup(fqdn, zk_root, dry_run=True)
    assert result.exit_code == 0, f" output:\n {result.output.decode().strip()}"


@when("we do hosts cleanup on {node} with fqdn {fqdn}")
def step_host_cleanup(context: ContextT, node: str, fqdn: str) -> None:
    container = get_container(context, node)
    result = Chadmin(container).zk_cleanup(fqdn, no_ch_config=False)
    assert result.exit_code == 0, f" output:\n {result.output.decode().strip()}"


@when(
    "we do table cleanup on {node} with fqdn {fqdn} from table with {zk_table_path} zookeeper path"
)
def step_table_cleanup(
    context: ContextT, node: str, fqdn: str, zk_table_path: str
) -> None:
    container = get_container(context, node)
    result = Chadmin(container).zk_cleanup_table(fqdn, zk_table_path)
    assert result.exit_code == 0, f" output:\n {result.output.decode().strip()}"


@then("the list of children on {node:w} for zk node {zk_node} are equal to")
def step_childen_list(context: ContextT, node: str, zk_node: str) -> None:
    container = get_container(context, node)
    result = Chadmin(container).zk_list(zk_node)
    assert_that(result.output.decode(), equal_to(context.text + "\n"))


@then("the list of children on {node:w} for zk node {zk_node} are empty")
def step_childen_list_empty(context: ContextT, node: str, zk_node: str) -> None:
    container = get_container(context, node)
    result = Chadmin(container).zk_list(zk_node)
    assert_that(result.output.decode(), equal_to("\n"))


@when("we delete zookeepers nodes {zk_nodes} on {node:w}")
def step_delete_command(context: ContextT, zk_nodes: str, node: str) -> None:
    container = get_container(context, node)
    result = Chadmin(container).zk_delete(zk_nodes)
    assert result.exit_code == 0, f" output:\n {result.output.decode().strip()}"
