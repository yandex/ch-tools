#!/usr/bin/env python3
import logging

import click

from cloud.mdb.clickhouse.tools.access_migrate.keeper import get_keeper_client
from cloud.mdb.clickhouse.tools.access_migrate.cli import local_to_replicated
from cloud.mdb.clickhouse.tools.access_migrate.cli import replicated_to_local
from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseConfig
from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseKeeperConfig

CLI_COMMANDS = [
    local_to_replicated,
    replicated_to_local,
]


@click.group('access')
@click.option('-d', '--debug', is_flag=True, help="Enable debug output.")
@click.pass_context
def cli(ctx, debug):
    """ClickHouse access migration tool."""
    if debug:
        logging.basicConfig(level='DEBUG', format='%(levelname)s:%(message)s')

    ch_config = ClickhouseConfig.load()
    ch_keeper_config = ClickhouseKeeperConfig.load()

    ctx.obj = dict(
        ch_config=ch_config,
        ch_keeper_config=ch_keeper_config,
        debug=debug,
    )
    ctx.obj['keeper_cli'] = get_keeper_client(ctx)

    @ctx.call_on_close
    def close_keeper():
        ctx.obj['keeper_cli'].close()


for command in CLI_COMMANDS:
    cli.add_command(command)


def main() -> None:
    cli()
