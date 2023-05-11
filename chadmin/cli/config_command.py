from click import command, pass_context

from common.clickhouse.config import ClickhouseConfig
from common.cli.formatting import print_response


@command('config')
@pass_context
def config_command(ctx):
    """
    Output ClickHouse config.
    """
    config = ClickhouseConfig.load()
    print_response(ctx, config.dump())
