"""
Commands for manipulating ClickHouse server.
"""

import time
from typing import Optional

from cloup import Context, group, option, pass_context

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.cli.wait_group import (
    is_initial_dictionaries_load_completed,
    warmup_system_users,
)
from ch_tools.chadmin.internal.utils import execute_query
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
    config = ctx.obj["config"]

    # Get parameters from config
    restart_cmd = config["chadmin"]["server"]["restart"]["command"]
    timeout_value = timeout or config["chadmin"]["server"]["restart"]["timeout"]
    check_interval = config["chadmin"]["server"]["restart"]["check_interval"]

    logging.info(f"Restarting ClickHouse server with command: {restart_cmd}")
    start_time = time.time()

    # Execute restart command
    try:
        execute(restart_cmd)
    except Exception as e:
        raise RuntimeError(f"Failed to execute restart command: {e}")

    logging.info("Waiting for ClickHouse server to restart...")
    deadline = start_time + timeout_value

    # Wait for server to restart by checking uptime
    while time.time() < deadline:
        try:
            uptime_result = execute_query(
                ctx, "SELECT uptime()", format_="TabSeparated", timeout=5
            )
            uptime = int(uptime_result.strip())
            elapsed = time.time() - start_time

            # If uptime is less than elapsed time, server has restarted
            if uptime < elapsed:
                logging.info(
                    f"ClickHouse server restarted successfully (uptime: {uptime}s)"
                )

                # Warm up system users and wait for dictionaries to load
                warmup_system_users(ctx)

                if is_initial_dictionaries_load_completed(
                    ctx, wait_failed_dictionaries=False
                ):
                    logging.info("Server is fully operational")
                    return
        except Exception as e:
            logging.debug(f"Server not ready yet: {e}")

        time.sleep(check_interval)

    raise RuntimeError(f"Server didn't fully start within {timeout_value} seconds")
