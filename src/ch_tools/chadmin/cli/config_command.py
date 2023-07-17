from click import command, pass_context

from ch_tools.common.cli.formatting import print_response
from ch_tools.common.clickhouse.config import ClickhouseConfig


@command("config")
@pass_context
def config_command(ctx):
    """
    Output ClickHouse config.
    """
    config = ClickhouseConfig.load()
    print_response(ctx, config.dump())
