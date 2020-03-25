"""
Common steps.
"""
import time

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


@when('we try to execute command on {node:w}')
def step_try_command(context, node):
    container = docker.get_container(context, node)
    context.command = context.text.strip()
    result = container.exec_run(['bash', '-c', context.command], user='root')
    context.response = result.output.decode()
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


@when('we sleep for {seconds:d} seconds')
def step_sleep(_context, seconds):
    time.sleep(seconds)
