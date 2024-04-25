import os
import pwd
import sys
import warnings
from functools import wraps
from typing import Optional

import click
import cloup
from cloup import group, option, pass_context, version_option

from ch_tools.common import logging
from ch_tools.common.config import CH_MONITORING_LOG_FILE, load_config

warnings.filterwarnings(action="ignore", message="Python 3.6 is no longer supported")

# pylint: disable=wrong-import-position

from ch_tools import __version__
from ch_tools.common.cli.context_settings import CONTEXT_SETTINGS
from ch_tools.common.cli.locale_resolver import LocaleResolver
from ch_tools.common.result import Status
from ch_tools.monrun_checks.ch_backup import backup_command
from ch_tools.monrun_checks.ch_core_dumps import core_dumps_command
from ch_tools.monrun_checks.ch_dist_tables import dist_tables_command
from ch_tools.monrun_checks.ch_geobase import geobase_command
from ch_tools.monrun_checks.ch_keeper import keeper_command
from ch_tools.monrun_checks.ch_log_errors import log_errors_command
from ch_tools.monrun_checks.ch_ping import ping_command
from ch_tools.monrun_checks.ch_replication_lag import replication_lag_command
from ch_tools.monrun_checks.ch_resetup_state import resetup_state_command
from ch_tools.monrun_checks.ch_ro_replica import ro_replica_command
from ch_tools.monrun_checks.ch_s3_backup_orphaned import orphaned_backups_command
from ch_tools.monrun_checks.ch_system_queues import system_queues_command
from ch_tools.monrun_checks.ch_tls import tls_command
from ch_tools.monrun_checks.dns import dns_command
from ch_tools.monrun_checks.exceptions import translate_to_status
from ch_tools.monrun_checks.status import status_command

DEFAULT_USER = "monitor"

# pylint: disable=too-many-ancestors


class MonrunChecks(cloup.Group):
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
        @pass_context
        def callback_wrapper(ctx, *args, **kwargs):
            logging.configure(
                ctx.obj["config"]["loguru"], "ch-monitoring", {"cmd_name": cmd.name}
            )

            logging.debug("Start executing")

            status = Status()
            try:
                result = ctx.invoke(cmd_callback, *args, **kwargs)
                if result:
                    status.append(result.message)
                    status.set_code(result.code)
                    if result.verbose:
                        status.add_verbose(result.verbose)
            except Exception as exc:
                if not isinstance(exc, UserWarning):
                    logging.exception("Got error:")
                status = translate_to_status(exc, status)

            log_message = f"Completed with {status.code};{status.message}"
            logging.disable_stdout_logger()
            logging.log_status(status.code, log_message)

            if ctx.obj and ctx.obj.get("status_mode", False):
                return status
            status.report(ctx)

        cmd.callback = callback_wrapper
        super().add_command(
            cmd,
            name=name,
            section=section,
            fallback_to_default_section=fallback_to_default_section,
        )


@group(
    cls=MonrunChecks,
    context_settings=CONTEXT_SETTINGS,
)
@option(
    "--ensure-monitoring-user/--no-ensure-monitoring-user",
    "ensure_monitoring_user",
    default=True,
    help="Ensure that monitoring checks are run under monitoring user.",
)
@version_option(__version__)
@pass_context
def cli(ctx, ensure_monitoring_user):
    config = load_config()

    if ensure_monitoring_user:
        _ensure_monitoring_user()

    ctx.obj = {
        "config": config,
        "monitoring": True,
    }
    ctx.default_map = config["ch-monitoring"]


CLI_COMMANDS = [
    ping_command,
    log_errors_command,
    replication_lag_command,
    system_queues_command,
    core_dumps_command,
    dist_tables_command,
    resetup_state_command,
    ro_replica_command,
    geobase_command,
    backup_command,
    orphaned_backups_command,
    tls_command,
    keeper_command,
    dns_command,
]

cli.add_command(status_command(CLI_COMMANDS))

for command in CLI_COMMANDS:
    cli.add_command(command)


def main():
    """
    Program entry point.
    """
    LocaleResolver.resolve()
    cli.main()


def _ensure_monitoring_user():
    euid = os.geteuid()
    user = pwd.getpwuid(euid).pw_name

    if user != DEFAULT_USER:
        if euid != 0:
            print(f"Wrong current user: {user}", file=sys.stderr)
            sys.exit(1)
        else:
            try:
                pw = pwd.getpwnam(DEFAULT_USER)
                if os.path.isfile(CH_MONITORING_LOG_FILE):
                    os.chown(CH_MONITORING_LOG_FILE, pw.pw_uid, pw.pw_gid)
                groups = os.getgrouplist(DEFAULT_USER, pw.pw_gid)
                os.setgroups(groups)
                os.setgid(pw.pw_gid)
                os.setegid(pw.pw_gid)
                os.setuid(pw.pw_uid)
                os.seteuid(pw.pw_uid)
            except Exception as exc:
                print(repr(exc), file=sys.stderr)
                sys.exit(1)


if __name__ == "__main__":
    main()
