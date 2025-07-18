import warnings
from functools import wraps
from typing import Any, Optional

import click
import cloup

from ch_tools.common.cli.parameters import YamlParamType
from ch_tools.common.utils import get_full_command_name, update_by_key_path

warnings.filterwarnings(action="ignore", message="Python 3.6 is no longer supported")

# pylint: disable=wrong-import-position

from ch_tools import __version__
from ch_tools.common import logging
from ch_tools.common.cli.context_settings import CONTEXT_SETTINGS
from ch_tools.common.cli.locale_resolver import LocaleResolver
from ch_tools.common.config import load_config
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
    tls_command,
)
from ch_tools.monrun_checks_keeper.status import status_command

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
        def wrapper(ctx: Any, *a: Any, **kw: Any) -> Any:
            logging.configure(
                ctx.obj["config"]["loguru"],
                "keeper-monitoring",
                {"cmd_name": get_full_command_name(ctx)},
            )

            logging.debug(
                "Command starts executing, params: {}, args: {}, version: {}",
                {
                    **ctx.parent.params,
                    **ctx.params,
                },
                ctx.args,
                __version__,
            )

            status = Status()
            try:
                result = ctx.invoke(cmd_callback, *a, **kw)
                logging.disable_stdout_logger()
                if result:
                    status.append(result.message)
                    status.set_code(result.code)
            except Exception as e:
                logging.disable_stdout_logger()
                logging.exception("Error occurred while executing:")
                status.append(repr(e))
                status.set_code(1)

            log_message = f"Completed with {status.code};{status.message}"
            logging.log_status(status.code, log_message)

            if ctx.obj and ctx.obj.get("status_mode", False):
                return status
            status.report(ctx)

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
@cloup.option(
    "--setting",
    "settings",
    multiple=True,
    type=(str, YamlParamType()),
    metavar="NAME VALUE",
    help="Name and value of tool setting to override. "
    "Can be specified multiple times to override several settings.",
)
@cloup.version_option(__version__)
@cloup.pass_context
def cli(
    ctx: click.Context,
    settings: Any,
    retries: int,
    timeout: float,
    no_verify_ssl_certs: bool,
) -> None:
    config = load_config()

    for setting_path, value in settings:
        update_by_key_path(config, setting_path, value)

    ctx.obj = {
        "config": config,
        "retries": retries,
        "timeout": timeout,
        "no_verify_ssl_certs": no_verify_ssl_certs,
        "monitoring": True,
    }
    ctx.default_map = config["keeper-monitoring"]


COMMANDS = [
    alive_command,
    avg_latency_command,
    min_latency_command,
    max_latency_command,
    queue_command,
    descriptors_command,
    get_version_command,
    check_snapshots,
    tls_command,
]

cli.add_command(status_command(COMMANDS))

for command in COMMANDS:
    cli.add_command(command)
cli.add_command(check_last_null_pointer_exc)


def main() -> None:
    """
    Program entry point.
    """
    LocaleResolver.resolve()
    cli.main()


if __name__ == "__main__":
    main()
