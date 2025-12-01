import json
import os
import subprocess
from typing import Any

import click
import psutil

from ch_tools.common.clickhouse.config.path import CLICKHOUSE_RESETUP_CONFIG_PATH
from ch_tools.common.result import CRIT, OK, Result
from ch_tools.monrun_checks.exceptions import die


# TODO: delete unused ssl and ca_bundle options after some time
@click.command("resetup-state")
@click.option("-s", "--ssl", "_ssl", is_flag=True, help="Use HTTPS rather than HTTP.")
@click.option("--ca_bundle", "_ca_bundle", help="Path to CA bundle to use.")
def resetup_state_command(_ssl: bool, _ca_bundle: Any) -> Any:
    """
    Check state of resetup process.
    """

    check_resetup_running()
    check_resetup_required()

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


def check_resetup_required() -> None:
    """
    Check resetup conditions
    """
    cmd = [
        "sudo",
        "salt-call",
        "mdb_clickhouse.resetup_required",
        "--out",
        "json",
        "--local",
    ]
    output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    if json.loads(output)["local"]:
        die(0, "OK")
