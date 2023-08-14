import getpass
import logging
import os
import pwd
import sys
import warnings
from functools import wraps
from typing import Optional

warnings.filterwarnings(action="ignore", message="Python 3.6 is no longer supported")

# pylint: disable=wrong-import-position

import click
import cloup

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
from ch_tools.monrun_checks.exceptions import translate_to_status
from ch_tools.monrun_checks.ext_ip_dns import ext_ip_dns_command
from ch_tools.monrun_checks.status import status_command

LOG_FILE = "/var/log/clickhouse-monitoring/clickhouse-monitoring.log"
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
        @cloup.pass_context
        def callback_wrapper(ctx, *args, **kwargs):
            os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
            logging.basicConfig(
                filename=LOG_FILE,
                level=logging.DEBUG,
                format=f"%(asctime)s %(process)-5d [%(levelname)s] {cmd.name}: %(message)s",
            )
            logging.getLogger("urllib3.connectionpool").setLevel(logging.CRITICAL)

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
                    logging.exception("Got error %s", repr(exc))
                status = translate_to_status(exc, status)

            log_message = f"Completed with {status.code};{status.message}"
            log_level = {0: logging.DEBUG, 1: logging.WARNING}.get(
                status.code, logging.ERROR
            )
            logging.log(log_level, log_message)

            if ctx.obj and ctx.obj.get("status_mode", False):
                return status
            status.report()

        cmd.callback = callback_wrapper
        super().add_command(
            cmd,
            name=name,
            section=section,
            fallback_to_default_section=fallback_to_default_section,
        )


@cloup.group(
    cls=MonrunChecks,
    context_settings=CONTEXT_SETTINGS,
)
@cloup.option(
    "--no-user-check",
    "no_user_check",
    is_flag=True,
    default=False,
    help="Do not check current user.",
)
@cloup.version_option(__version__)
def cli(no_user_check):
    if not no_user_check:
        check_current_user()


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
    ext_ip_dns_command,
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


def check_current_user():
    user = getpass.getuser()
    if user != DEFAULT_USER:
        if os.geteuid() != 0:
            print(f"Wrong current user: {user}", file=sys.stderr)
            sys.exit(1)
        else:
            try:
                pw = pwd.getpwnam(DEFAULT_USER)
                if os.path.isfile(LOG_FILE):
                    os.chown(LOG_FILE, pw.pw_uid, pw.pw_gid)
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
