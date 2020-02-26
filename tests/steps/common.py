"""
Common steps.
"""
import yaml
from behave import given, then, when
from hamcrest import assert_that, equal_to

from modules import docker
from modules.utils import merge


@given('default configuration')
@given('configuration')
def step_configuration(context):
    default: dict = {}
    overridden_options = yaml.load(context.text or '', yaml.SafeLoader) or {}
    for key, value in merge(default, overridden_options).items():
        context.__setattr__(key, value)


@given('we have executed command on {node:w}')
@when('we execute command on {node:w}')
def step_command(context, node):
    container = docker.get_container(context, node)
    command = context.text.strip()
    result = container.exec_run(['bash', '-c', command], user='root')
    context.response = result.output.decode()
    assert result.exit_code == 0, f'"{command}" failed with exit code {result.exit_code}, output:\n {context.response}'


@then('we get response')
def step_get_response(context):
    assert_that(context.response, equal_to(context.text))
