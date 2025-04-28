#!/usr/bin/env python3
"""
Manage test environment.
"""

import argparse
import pickle
from types import SimpleNamespace

import configuration
from behave import Context
from modules import compose, docker, minio, templates

from ch_tools.common import logging

SESSION_STATE_CONF = ".session_conf.sav"
STAGES = {
    "create": [
        templates.render_docker_configs,
        compose.create_config,
        compose.build_images,
    ],
    "start": [
        docker.create_network,
        compose.startup_containers,
        minio.initialize,
    ],
    "restart": [
        compose.shutdown_containers,
        docker.create_network,
        compose.startup_containers,
    ],
    "stop": [
        compose.shutdown_containers,
        docker.shutdown_network,
    ],
}


def create(context: Context) -> None:
    """
    Create test environment.
    """
    _run_stage("create", context)

    with open(context.state_file, "wb") as session_conf:
        pickle.dump(context.conf, session_conf)


def start(context: Context) -> None:
    """
    Start test environment runtime.
    """
    _run_stage("start", context)


def restart(context: Context) -> None:
    """
    Restart test environment runtime.
    """
    _run_stage("restart", context)


def stop(context: Context) -> None:
    """
    Stop test environment runtime.
    """
    _run_stage("stop", context)


def _run_stage(stage, context: Context) -> None:
    """
    Run stage steps.
    """
    assert stage in STAGES, stage + " not implemented"

    _init_context(context)

    for step in STAGES[stage]:
        step(context)


def _init_context(context: Context) -> None:
    """
    Initialize context.
    """
    if getattr(context, "initialized", False):
        return

    if not hasattr(context, "state_file"):
        context.state_file = SESSION_STATE_CONF

    try:
        with open(context.state_file, "rb") as session_conf:
            context.conf = pickle.load(session_conf)
    except FileNotFoundError:
        logging.info("creating new test config")
        context.conf = configuration.create()


def cli_main() -> None:
    """
    CLI entry.
    """
    commands = {
        "create": create,
        "start": start,
        "stop": stop,
    }

    args = _parse_args(commands)

    context = SimpleNamespace(state_file=args.state_file)

    commands[args.command](context)


def _parse_args(commands):
    """
    Parse command-line arguments.
    """
    arg = argparse.ArgumentParser(
        description="DBaaS testing environment initializer script"
    )
    arg.add_argument("command", choices=list(commands), help="command to perform")
    arg.add_argument(
        "-s",
        "--state-file",
        dest="state_file",
        type=str,
        metavar="<path>",
        default=SESSION_STATE_CONF,
        help="path to state file (pickle dump)",
    )
    return arg.parse_args()


if __name__ == "__main__":
    cli_main()
