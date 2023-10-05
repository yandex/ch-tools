"""
Steps for interacting with chadmin.
"""

from behave import then, when
from hamcrest import assert_that, equal_to
from modules.chadmin import Chadmin
from modules.docker import get_container


@when("we execute chadmin create zk nodes on {node:w}")
def step_create_(context, node):
    container = get_container(context, node)
    nodes = context.text.strip().split("\n")
    chadmin = Chadmin(container)

    for node in nodes:
        result = chadmin.create_zk_node(node)
        assert result.exit_code == 0, f" output:\n {result.output.decode().strip()}"


@when("we do hosts cleanup on {node} with fqdn {fqdn} and zk root {zk_root}")
def step_host_cleanup(context, node, fqdn, zk_root):
    container = get_container(context, node)
    result = Chadmin(container).zk_cleanup(fqdn, zk_root)
    assert result.exit_code == 0, f" output:\n {result.output.decode().strip()}"


@then("the list of children on {node:w} for zk node {zk_node} are equal to")
def step_childen_list(context, node, zk_node):
    container = get_container(context, node)
    result = Chadmin(container).zk_list(zk_node)
    assert_that(result.output.decode(), equal_to(context.text + "\n"))


@then("the list of children on {node:w} for zk node {zk_node} are empty")
def step_childen_list_empty(context, node, zk_node):
    container = get_container(context, node)
    result = Chadmin(container).zk_list(zk_node)
    assert_that(result.output.decode(), equal_to("\n"))
