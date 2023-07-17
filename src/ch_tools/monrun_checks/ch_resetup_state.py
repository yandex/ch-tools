import json
import os
import socket
import subprocess

import click
import psutil
import requests

from ch_tools.common.clickhouse.config.path import CLICKHOUSE_RESETUP_CONFIG_PATH
from ch_tools.common.result import Result
from ch_tools.monrun_checks.exceptions import die


@click.command("resetup-state")
@click.option("-p", "--port", "port", type=int, help="ClickHouse HTTP(S) port to use.")
@click.option("-s", "--ssl", "ssl", is_flag=True, help="Use HTTPS rather than HTTP.")
@click.option("--ca_bundle", "ca_bundle", help="Path to CA bundle to use.")
def resetup_state_command(port, ssl, ca_bundle):
    """
    Check state of resetup process.
    """

    check_repsync_running()
    check_resetup_running()
    check_resetup_required()

    host = socket.getfqdn()
    if request(host, port, ssl, ca_bundle):
        return Result(2, "ClickHouse is listening on ports reserved for resetup")

    if os.path.isfile(CLICKHOUSE_RESETUP_CONFIG_PATH):
        return Result(2, "Detected resetup config, but ch-backup is not running")

    return Result(0, "OK")


def check_resetup_running():
    """
    Check for currently running `ch-backup restore-schema`
    """
    for proc in psutil.process_iter():
        if {"/usr/bin/ch-backup", "restore-schema"}.issubset(proc.cmdline()):
            die(0, "resetup is running")


def check_repsync_running():
    """
    Check for currently running ch_wait_replication_sync.py script
    """
    for proc in psutil.process_iter():
        if {"/usr/local/yandex/ch_wait_replication_sync.py"}.issubset(proc.cmdline()):
            die(0, "resetup is running (wait for replication sync)")


def check_resetup_required():
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


def request(host, port, ssl, ca_bundle, query=None):
    """
    Send request to ClickHouse.
    """
    try:
        protocol = "https" if ssl else "http"
        verify = ca_bundle if ca_bundle else ssl
        params = {}
        if query:
            params["query"] = query

        r = requests.get(
            "{0}://{1}:{2}".format(protocol, host, port),
            params=params,
            headers={
                "X-ClickHouse-User": "mdb_monitor",
            },
            timeout=1,
            verify=verify,
        )
        return r.status_code == 200 and r.text.strip() == "Ok."
    except Exception:
        die(0, "OK")
