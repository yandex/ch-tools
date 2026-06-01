import os
from typing import Any

import click
import psutil

from ch_tools.common.clickhouse.config.path import CLICKHOUSE_RESETUP_CONFIG_PATH
from ch_tools.common.result import CRIT, OK, Result
from ch_tools.monrun_checks.exceptions import die


@click.command("resetup-state")
def resetup_state_command() -> Any:
    """
    Check state of resetup process.
    """

    check_resetup_running()

    if os.path.isfile(CLICKHOUSE_RESETUP_CONFIG_PATH):
        return Result(
            CRIT, "Detected resetup config, but couldn't find running resetup process"
        )

    return Result(OK)


def check_resetup_running() -> None:
    """
    Check for currently running resetup
    """
    for proc in psutil.process_iter():
        if {"/usr/bin/ch-backup", "restore-schema"}.issubset(proc.cmdline()):
            die(0, "resetup is running (restore schema)")
        if {"/usr/bin/chadmin", "wait", "replication-sync"}.issubset(proc.cmdline()):
            die(0, "resetup is running (wait for replication sync)")
