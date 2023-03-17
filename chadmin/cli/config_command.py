from click import command, pass_context

from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseConfig
from cloud.mdb.internal.python.cli.formatting import print_response


@command('config')
@pass_context
def config_command(ctx):
    """
    Output ClickHouse config.
    """
    config = ClickhouseConfig.load()
    print_response(ctx, config.dump())
