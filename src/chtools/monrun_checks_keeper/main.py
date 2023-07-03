import click
from functools import wraps
import logging

from click import group, option, pass_context
from chtools.common.result import Status
from chtools.monrun_checks_keeper.status import status_command
from chtools.monrun_checks_keeper.keeper_commands import (
    alive_command,
    avg_latency_command,
    min_latency_command,
    max_latency_command,
    queue_command,
    descriptors_command,
    get_version_command,
    check_snapshots,
    check_last_null_pointer_exc,
)

LOG_FILE = "/var/log/keeper-monitoring/keeper-monitoring.log"


class KeeperChecks(click.Group):
    def add_command(self, cmd, name=None):
        cmd_callback = cmd.callback

        @wraps(cmd_callback)
        @click.pass_context
        def wrapper(ctx, *a, **kw):
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
        super().add_command(cmd, name=name)


@group(cls=KeeperChecks, context_settings={"help_option_names": ["-h", "--help"]})
@option("-r", "--retries", "retries", type=int, default=3, help="Number of retries")
@option(
    "-t",
    "--timeout",
    "timeout",
    type=float,
    default=0.5,
    help="Connection timeout (in seconds)",
)
@option(
    "-n",
    "--no-verify-ssl-certs",
    "no_verify_ssl_certs",
    is_flag=True,
    default=False,
    help="Allow unverified SSL certificates, e.g. self-signed ones",
)
@pass_context
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

for cmd in COMMANDS:
    cli.add_command(cmd)
cli.add_command(check_last_null_pointer_exc)


def main():
    """
    Program entry point.
    """
    cli()
