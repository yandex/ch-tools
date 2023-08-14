import logging
import os
from functools import wraps
from typing import Optional

import click
import cloup

from ch_tools import __version__
from ch_tools.common.cli.context_settings import CONTEXT_SETTINGS
from ch_tools.common.cli.locale_resolver import LocaleResolver
from ch_tools.common.result import Status
from ch_tools.monrun_checks_keeper.keeper_commands import (
    alive_command,
    avg_latency_command,
    check_last_null_pointer_exc,
    check_snapshots,
    descriptors_command,
    get_version_command,
    max_latency_command,
    min_latency_command,
    queue_command,
)
from ch_tools.monrun_checks_keeper.status import status_command

LOG_FILE = "/var/log/keeper-monitoring/keeper-monitoring.log"

# pylint: disable=too-many-ancestors


class KeeperChecks(cloup.Group):
    def add_command(
        self,
        cmd: click.Command,
        name: Optional[str] = None,
        section: Optional[cloup.Section] = None,
        fallback_to_default_section: bool = True,
    ) -> None:
        if cmd.callback is None:
            super().add_command(
                cmd,
                name=name,
                section=section,
                fallback_to_default_section=fallback_to_default_section,
            )
            return

        cmd_callback = cmd.callback

        @wraps(cmd_callback)
        @cloup.pass_context
        def wrapper(ctx, *a, **kw):
            os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
            logging.basicConfig(
                filename=LOG_FILE,
                level=logging.DEBUG,
                format=f"%(asctime)s %(process)-5d [%(levelname)s] {cmd.name}: %(message)s",
            )
            logging.debug("Start executing")

            status = Status()
            try:
                result = ctx.invoke(cmd_callback, *a, **kw)
                if result:
                    status.append(result.message)
                    status.set_code(result.code)
            except Exception as e:
                logging.exception("Error occurred while executing:")
                status.append(repr(e))
                status.set_code(1)

            log_message = f"Completed with {status.code};{status.message}"
            logging.log(
                logging.DEBUG if status.code == 0 else logging.ERROR, log_message
            )

            if ctx.obj and ctx.obj.get("status_mode", False):
                return status
            status.report()

        cmd.callback = wrapper
        super().add_command(
            cmd,
            name=name,
            section=section,
            fallback_to_default_section=fallback_to_default_section,
        )


@cloup.group(
    cls=KeeperChecks,
    context_settings=CONTEXT_SETTINGS,
)
@cloup.option(
    "-r", "--retries", "retries", type=int, default=3, help="Number of retries"
)
@cloup.option(
    "-t",
    "--timeout",
    "timeout",
    type=float,
    default=0.5,
    help="Connection timeout (in seconds)",
)
@cloup.option(
    "-n",
    "--no-verify-ssl-certs",
    "no_verify_ssl_certs",
    is_flag=True,
    default=False,
    help="Allow unverified SSL certificates, e.g. self-signed ones",
)
@cloup.version_option(__version__)
@cloup.pass_context
def cli(ctx, retries, timeout, no_verify_ssl_certs):
    ctx.obj = dict(
        retries=retries, timeout=timeout, no_verify_ssl_certs=no_verify_ssl_certs
    )


COMMANDS = [
    alive_command,
    avg_latency_command,
    min_latency_command,
    max_latency_command,
    queue_command,
    descriptors_command,
    get_version_command,
    check_snapshots,
]

cli.add_command(status_command(COMMANDS))

for command in COMMANDS:
    cli.add_command(command)
cli.add_command(check_last_null_pointer_exc)


def main():
    """
    Program entry point.
    """
    LocaleResolver.resolve()
    cli.main()


if __name__ == "__main__":
    main()
