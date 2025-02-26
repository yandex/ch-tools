"""
Common steps.
"""

import time

import requests
import yaml
from behave import given, then, when
from hamcrest import assert_that, contains_string, equal_to, is_not, matches_regexp
from modules import docker
from modules.clickhouse import get_version
from modules.utils import merge


@given("default configuration")
@given("configuration")
def step_configuration(context):
    default: dict = {}
    overridden_options = yaml.load(context.text or "", yaml.SafeLoader) or {}
    for key, value in merge(default, overridden_options).items():
        setattr(context, key, value)


@when("we try to execute command on {node:w}")
def step_try_command(context, node):
    container = docker.get_container(context, node)
    context.command = context.text.strip()
    result = container.exec_run(["bash", "-c", context.command], user="root")
    context.response = result.output.decode().strip()
    context.exit_code = result.exit_code


@given("we have executed command on {node:w}")
@when("we execute command on {node:w}")
def step_command(context, node):
    step_try_command(context, node)
    assert context.exit_code == 0, (
        f'"{context.command}" failed with exit code {context.exit_code},'
        f" output:\n {context.response}"
    )


@then("it completes successfully")
def step_complete_successfully(context):
    assert context.exit_code == 0


@then("it fails")
def step_command_fail(context):
    assert (
        context.exit_code != 0
    ), f'"{context.command}" succeeded, but failure was expected'


@then("it fails with response contains")
def step_command_fail_response_contains(context):
    assert (
        context.exit_code != 0
    ), f'"{context.command}" succeeded, but failure was expected'
    assert_that(context.response, contains_string(context.text))


@then("we get response")
def step_get_response(context):
    assert_that(context.response, equal_to(context.text))


@then("we get query response")
def step_get_query_response(context):
    assert_that(
        context.response,
        equal_to(context.text.replace("\\t", "\t").replace("\\n", "\n")),
    )


@then("we get response contains")
def step_get_response_contains(context):
    assert_that(context.response, contains_string(context.text))


@then("we get response matches")
def step_get_response_matches(context):
    assert_that(context.response, matches_regexp(context.text))


@then("we get response not contains {entry:w}")
def step_get_response_not_contains(context, entry):
    assert_that(context.response, is_not(contains_string(entry)))  # type: ignore


@when('we create file {file_path} with data "{data}"')
def step_create_file(context, file_path, data):
    container = docker.get_container(context, "clickhouse01")
    result = container.exec_run(
        ["bash", "-c", f'echo "{data}" > {file_path}'], user="root"
    )
    assert result.exit_code == 0


@then("we get file {file_path}")
def step_get_file(context, file_path):
    container = docker.get_container(context, "clickhouse01")
    result = container.exec_run(["bash", "-c", f"cat {file_path}"], user="root")
    context.response = result.output.decode().strip()
    assert result.exit_code == 0
    assert_that(context.response, equal_to(context.text))


@when("we sleep for {seconds:d} seconds")
def step_sleep(_context, seconds):
    time.sleep(seconds)


@given("a working http server")
def working_http(context):
    """
    Ensure that http server is ready to accept incoming requests.
    """
    # pylint: disable=missing-timeout
    container = docker.get_container(context, "http_mock01")
    host, port = docker.get_exposed_port(container, 8080)
    response = requests.get(f"http://{host}:{port}/")
    assert response.text == "OK", f'expected "OK", got "{response.text}"'


@given("installed clickhouse-tools config with version on {node:w}")
def install_ch_tools_config_with_version(context, node):
    version = get_version(context, node)
    container = docker.get_container(context, node)
    config_data = yaml.dump({"clickhouse": {"version": version["data"][0][0]}})

    container.exec_run(["bash", "-c", "mkdir /etc/clickhouse-tools"])
    container.exec_run(
        ["bash", "-c", f"echo '{config_data}' > /etc/clickhouse-tools/config.yaml"]
    )


@given("installed clickhouse-tools config with user on {nodes}")
def install_ch_tools_config_with_user(context, nodes):
    nodes_list = nodes.split(",")
    user = "_admin"
    password = ""

    for node in nodes_list:
        container = docker.get_container(context, node)
        config_data = yaml.dump({"clickhouse": {"user": user, "password": password}})

        container.exec_run(["bash", "-c", "mkdir /etc/clickhouse-tools"])
        container.exec_run(
            ["bash", "-c", f"echo '{config_data}' > /etc/clickhouse-tools/config.yaml"]
        )
