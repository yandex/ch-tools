from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseConfig


def get_config(ctx) -> ClickhouseConfig:
    if 'config' not in ctx.obj:
        ctx.obj['config'] = ClickhouseConfig.load()

    return ctx.obj['config']


def get_macros(ctx):
    return get_config(ctx).macros


def get_cluster_name(ctx):
    return get_config(ctx).cluster_name
