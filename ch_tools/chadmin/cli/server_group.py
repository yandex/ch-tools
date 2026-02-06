"""
Commands for manipulating ClickHouse server.
"""

import time
from typing import Optional

from cloup import Context, group, option, pass_context

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.cli.wait_group import wait_started_command
from ch_tools.common import logging
from ch_tools.common.utils import execute


@group("server", cls=Chadmin)
def server_group() -> None:
    """Commands for manipulating ClickHouse server (restart|etc.)."""
    pass


@server_group.command("restart")
@option(
    "--timeout",
    type=int,
    help="Maximum time to wait for server restart, in seconds.",
)
@pass_context
def restart_command(ctx: Context, timeout: Optional[int]) -> None:
    """Restart ClickHouse server and wait for it to start."""

    # Get parameters from config with safe defaults
    restart_config = ctx.obj["config"]["chadmin"]["server"]["restart"]
    restart_cmd = restart_config["command"]
    default_timeout = restart_config["timeout"]

    # Apply CLI timeout override if provided
    timeout_value = timeout if timeout is not None else default_timeout

    logging.info(f"Restarting ClickHouse server with command: {restart_cmd}")
    logging.info(f"Using timeout: {timeout_value}s")
    start_time = time.time()

    # Execute restart command
    try:
        execute(restart_cmd)
    except Exception as e:
        raise RuntimeError(f"Failed to execute restart command: {e}")

    logging.info("Waiting for ClickHouse server to restart...")

    # Call wait_started_command programmatically with restart tracking enabled
    ctx.invoke(
        wait_started_command,
        quiet=False,
        wait_failed_dictionaries=False,
        track_pid_file="",
        track_restart=True,
        restart_start_time=start_time,
        timeout_strategy="files",
        file_processing_speed=None,
        min_timeout=None,
        max_timeout=None,
        wait=timeout_value,
    )

    logging.info("Server is fully operational")
