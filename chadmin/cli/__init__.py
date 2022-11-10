from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseConfig


def get_config(ctx) -> ClickhouseConfig:
    if 'clickhouse_config' not in ctx.obj:
        ctx.obj['clickhouse_config'] = ClickhouseConfig.load()

    return ctx.obj['clickhouse_config']


def get_macros(ctx):
    return get_config(ctx).macros


def get_cluster_name(ctx):
    return get_config(ctx).cluster_name
