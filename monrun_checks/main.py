import logging
from functools import wraps
import click

from cloud.mdb.clickhouse.tools.monrun_checks.result import Result, Status
from cloud.mdb.clickhouse.tools.monrun_checks.ch_replication_lag import replication_lag_command
from cloud.mdb.clickhouse.tools.monrun_checks.ch_system_queues import system_queues_command
from cloud.mdb.clickhouse.tools.monrun_checks.ch_core_dumps import core_dumps_command
from cloud.mdb.clickhouse.tools.monrun_checks.ch_dist_tables import dist_tables_command
from cloud.mdb.clickhouse.tools.monrun_checks.ch_resetup_state import resetup_state_command
from cloud.mdb.clickhouse.tools.monrun_checks.ch_ro_replica import ro_replica_command
from cloud.mdb.clickhouse.tools.monrun_checks.ch_geobase import geobase_command
from cloud.mdb.clickhouse.tools.monrun_checks.ch_log_errors import log_errors_command
from cloud.mdb.clickhouse.tools.monrun_checks.ch_ping import ping_command

LOG_FILE = '/var/log/monrun/clickhouse-monitoring.log'


class MonrunChecks(click.Group):

    def add_command(self, cmd, name=None):
        cmd_callback = cmd.callback

        @wraps(cmd_callback)
        @click.pass_context
        def callback_wrapper(ctx, *args, **kwargs):
            status = Status()
            try:
                result = ctx.invoke(cmd_callback, *args, **kwargs)
                status.append(result.message)
                status.set_code(result.code)
            except UserWarning as exc:
                code, message = exc.args
                status.append(message)
                status.set_code(code)
            except Exception as exc:
                status.append(repr(exc))
                status.set_code(2)

            status.report()

        cmd.callback = callback_wrapper
        super().add_command(cmd, name=name)


@click.group(cls=MonrunChecks)
@click.option('--debug', is_flag=True, help='Enable debug output.')
def cli(debug):
    loglevel = 'DEBUG' if debug else 'CRITICAL'
    logging.basicConfig(
        filename=LOG_FILE,
        level=loglevel,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')


cli.add_command(replication_lag_command)
cli.add_command(system_queues_command)
cli.add_command(core_dumps_command)
cli.add_command(dist_tables_command)
cli.add_command(resetup_state_command)
cli.add_command(ro_replica_command)
cli.add_command(geobase_command)
cli.add_command(log_errors_command)
cli.add_command(ping_command)


def main():
    cli()
