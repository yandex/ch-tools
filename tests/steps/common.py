"""
Common steps.
"""
import time

import requests
import yaml
from behave import given, then, when
from hamcrest import assert_that, contains_string, equal_to

from modules import docker
from modules.utils import merge


@given('default configuration')
@given('configuration')
def step_configuration(context):
    default: dict = {}
    overridden_options = yaml.load(context.text or '', yaml.SafeLoader) or {}
    for key, value in merge(default, overridden_options).items():
        context.__setattr__(key, value)


@when('we try to execute command on {node:w}')
def step_try_command(context, node):
    container = docker.get_container(context, node)
    context.command = context.text.strip()
    result = container.exec_run(['bash', '-c', context.command], user='root')
    context.response = result.output.decode().strip()
    context.exit_code = result.exit_code


@given('we have executed command on {node:w}')
@when('we execute command on {node:w}')
def step_command(context, node):
    step_try_command(context, node)
    assert context.exit_code == 0, (f'"{context.command}" failed with exit code {context.exit_code},'
                                    f' output:\n {context.response}')


@then('it fails')
def step_command_fail(context):
    assert context.exit_code != 0, f'"{context.command}" succeeded, but failure was expected'


@then('we get response')
def step_get_response(context):
    assert_that(context.response, equal_to(context.text))


@then('we get response contains')
def step_get_response_contains(context):
    assert_that(context.response, contains_string(context.text))


@when('we sleep for {seconds:d} seconds')
def step_sleep(_context, seconds):
    time.sleep(seconds)


@given('a working http server')
def working_http(context):
    """
    Ensure that http server is ready to accept incoming requests.
    """
    container = docker.get_container(context, 'http_mock01')
    host, port = docker.get_exposed_port(container, 8080)
    response = requests.get(f'http://{host}:{port}/')
    assert response.text == 'OK', f'expected "OK", got "{response.text}"'
