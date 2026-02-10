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
from modules.typing import ContextT
from modules.utils import merge


@given("default configuration")
@given("configuration")
def step_configuration(context: ContextT) -> None:
    default: dict = {}
    overridden_options = yaml.load(context.text or "", yaml.SafeLoader) or {}
    for key, value in merge(default, overridden_options).items():
        setattr(context, key, value)


@when("we try to execute command on {node:w}")
def step_try_command(context: ContextT, node: str) -> None:
    container = docker.get_container(context, node)
    context.command = context.text.strip()
    result = container.exec_run(["bash", "-c", context.command], user="root")
    context.response = result.output.decode().strip()
    context.exit_code = result.exit_code


@given("we have executed command on {node:w}")
@when("we execute command on {node:w}")
def step_command(context: ContextT, node: str) -> None:
    step_try_command(context, node)
    assert context.exit_code == 0, (
        f'"{context.command}" failed with exit code {context.exit_code},'
        f" output:\n {context.response}"
    )


@then("it completes successfully")
def step_complete_successfully(context: ContextT) -> None:
    assert context.exit_code == 0


@then("it fails")
def step_command_fail(context: ContextT) -> None:
    assert (
        context.exit_code != 0
    ), f'"{context.command}" succeeded, but failure was expected'


@then("it fails with response contains")
def step_command_fail_response_contains(context: ContextT) -> None:
    assert (
        context.exit_code != 0
    ), f'"{context.command}" succeeded, but failure was expected'
    assert_that(context.response, contains_string(context.text))


@then("we get response")
def step_get_response(context: ContextT) -> None:
    assert_that(context.response, equal_to(context.text))


@then("we get query response")
def step_get_query_response(context: ContextT) -> None:
    assert_that(
        context.response,
        equal_to(context.text.replace("\\t", "\t").replace("\\n", "\n")),
    )


@then("we get response contains")
def step_get_response_contains(context: ContextT) -> None:
    assert_that(context.response, contains_string(context.text))


@then("we get response matches")
def step_get_response_matches(context: ContextT) -> None:
    assert_that(context.response, matches_regexp(context.text))


@then("we get response not contains {entry:w}")
def step_get_response_not_contains(context: ContextT, entry: str) -> None:
    assert_that(context.response, is_not(contains_string(entry)))  # type: ignore


@then("we get response does not contain")
def step_get_response_does_not_contain(context: ContextT) -> None:
    assert_that(context.response, is_not(contains_string(context.text)))  # type: ignore


@when('we create file {file_path} with data "{data}"')
def step_create_file(context: ContextT, file_path: str, data: str) -> None:
    container = docker.get_container(context, "clickhouse01")
    result = container.exec_run(
        ["bash", "-c", f'echo "{data}" > {file_path}'], user="root"
    )
    assert result.exit_code == 0


@when("we create file {file_path} on {node:w} with content")
def step_create_file_with_content(context: ContextT, file_path: str, node: str) -> None:
    """Create a file with multiline content from context.text"""
    container = docker.get_container(context, node)
    content = context.text
    # Escape single quotes and use cat with heredoc for multiline content
    result = container.exec_run(
        ["bash", "-c", f"cat > {file_path} << 'EOF'\n{content}\nEOF"], user="root"
    )
    assert (
        result.exit_code == 0
    ), f"Failed to create file {file_path}: {result.output.decode()}"


@then("we get file {file_path}")
def step_get_file(context: ContextT, file_path: str) -> None:
    container = docker.get_container(context, "clickhouse01")
    result = container.exec_run(["bash", "-c", f"cat {file_path}"], user="root")
    context.response = result.output.decode().strip()
    assert result.exit_code == 0
    assert_that(context.response, equal_to(context.text))


@when("we sleep for {seconds:d} seconds")
def step_sleep(_context: ContextT, seconds: int) -> None:
    time.sleep(seconds)


@given("a working http server")
def working_http(context: ContextT) -> None:
    """
    Ensure that http server is ready to accept incoming requests.
    """
    # pylint: disable=missing-timeout
    container = docker.get_container(context, "http_mock01")
    host, port = docker.get_exposed_port(container, 8080)
    response = requests.get(f"http://{host}:{port}/")
    assert response.text == "OK", f'expected "OK", got "{response.text}"'


@given("installed clickhouse-tools config with version on {node:w}")
def install_ch_tools_config_with_version(context: ContextT, node: str) -> None:
    version = get_version(context, node)
    container = docker.get_container(context, node)
    config_data = yaml.dump({"clickhouse": {"version": version["data"][0][0]}})

    container.exec_run(["bash", "-c", "mkdir /etc/clickhouse-tools"])
    container.exec_run(
        ["bash", "-c", f"echo '{config_data}' > /etc/clickhouse-tools/config.yaml"]
    )


@given("clickhouse-tools configuration on {nodes}")
def clickhouse_tools_configuration(context: ContextT, nodes: str) -> None:
    nodes_list = nodes.split(",")
    conf = context.text

    for node in nodes_list:
        container = docker.get_container(context, node)
        container.exec_run(["bash", "-c", "mkdir /etc/clickhouse-tools"])
        container.exec_run(
            ["bash", "-c", f"echo '{conf}' > /etc/clickhouse-tools/config.yaml"]
        )


@given("merged clickhouse-tools configuration on {nodes}")
def merged_clickhouse_tools_configuration(context: ContextT, nodes: str) -> None:
    nodes_list = nodes.split(",")
    new_conf = yaml.load(context.text, yaml.SafeLoader) or {}

    for node in nodes_list:
        container = docker.get_container(context, node)
        container.exec_run(["bash", "-c", "mkdir -p /etc/clickhouse-tools"])

        result = container.exec_run(
            [
                "bash",
                "-c",
                "cat /etc/clickhouse-tools/config.yaml 2>/dev/null || echo '{}'",
            ]
        )
        existing_conf = yaml.load(result.output.decode(), yaml.SafeLoader) or {}

        merged_conf = merge(existing_conf, new_conf)
        merged_yaml = yaml.dump(merged_conf)

        container.exec_run(
            ["bash", "-c", f"echo '{merged_yaml}' > /etc/clickhouse-tools/config.yaml"]
        )
