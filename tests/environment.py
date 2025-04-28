"""
Behave entry point.
"""

import re
import sys
from typing import Optional

import env_control
from behave import model
from modules.logs import save_logs
from modules.typing import ContextT
from modules.utils import version_ge, version_lt

from ch_tools.common import logging
from ch_tools.common.config import load_config

try:
    import ipdb as pdb
except ImportError:
    import pdb  # type: ignore


def before_all(context: ContextT) -> None:
    """
    Prepare environment for tests.
    """
    config = load_config()
    logging.configure(config["loguru"], "test")
    logging.add(
        sys.stdout,
        level="INFO",
        format_="{time:YYYY-MM-DD HH:mm:ss,SSS} [{level:8}]:\t{message}",
    )
    if not context.config.userdata.getbool("skip_setup"):
        env_control.create(context)


def before_feature(context: ContextT, _feature: model.Feature) -> None:
    """
    Cleanup function executing per feature.
    """
    if "dependent-scenarios" in _feature.tags:
        env_control.restart(context)


def before_scenario(context: ContextT, scenario: model.Scenario) -> None:
    """
    Cleanup function executing per scenario.
    """
    if "dependent-scenarios" not in context.feature.tags and _check_tags(
        context, scenario
    ):
        env_control.restart(context)


def after_step(context: ContextT, step: model.Step) -> None:
    """
    Save logs after failed step.
    """
    if step.status == "failed":
        save_logs(context)
        if context.config.userdata.getbool("debug"):
            pdb.post_mortem(step.exc_traceback)


def after_all(context: ContextT) -> None:
    """
    Clean up.
    """
    if (context.failed and not context.aborted) and context.config.userdata.getbool(
        "no_stop_on_fail"
    ):
        logging.info("Not stopping containers on failure as requested")
        return
    env_control.stop(context)


def _check_tags(context: ContextT, scenario: model.Scenario) -> bool:
    ch_version = context.conf["ch_version"]

    require_version = _parse_version_tag(scenario.tags, "require_version")
    if require_version:
        if not version_ge(ch_version, require_version):
            logging.info("Skipping scenario due to require_version mismatch")
            scenario.mark_skipped()
            return False

    require_lt_version = _parse_version_tag(scenario.tags, "require_version_less_than")
    if require_lt_version:
        if not version_lt(ch_version, require_lt_version):
            logging.info("Skipping scenario due to require_version_less_than mismatch")
            scenario.mark_skipped()
            return False

    if "skip" in scenario.tags:
        logging.info("Skipping scenario due to skip tag")
        scenario.mark_skipped()
        return False

    return True


def _parse_version_tag(tags: list, prefix: str) -> Optional[str]:
    tag_pattern = prefix + r"_(?P<version>[\d\.]+)"
    for tag in tags:
        match = re.fullmatch(tag_pattern, tag)
        if match:
            return match.group("version")

    return None
