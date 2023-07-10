"""
Behave entry point.
"""
import logging

import env_control
from modules.logs import save_logs

REQUIRE_VERSION_PREFIX_LEN = len("require_version_")

try:
    import ipdb as pdb
except ImportError:
    import pdb  # type: ignore


def before_all(context):
    """
    Prepare environment for tests.
    """
    if not context.config.userdata.getbool("skip_setup"):
        env_control.create(context)


def before_feature(context, _feature):
    """
    Cleanup function executing per feature.
    """
    if "dependent-scenarios" in _feature.tags:
        env_control.restart(context)


def before_scenario(context, _scenario):
    """
    Cleanup function executing per scenario.
    """
    if "dependent-scenarios" not in context.feature.tags:
        env_control.restart(context)


def after_step(context, step):
    """
    Save logs after failed step.
    """
    if step.status == "failed":
        save_logs(context)
        if context.config.userdata.getbool("debug"):
            pdb.post_mortem(step.exc_traceback)


def after_all(context):
    """
    Clean up.
    """
    if (context.failed and not context.aborted) and context.config.userdata.getbool(
        "no_stop_on_fail"
    ):
        logging.info("Not stopping containers on failure as requested")
        return
    env_control.stop(context)
