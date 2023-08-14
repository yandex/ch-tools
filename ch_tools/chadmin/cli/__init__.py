from click import Context

from ch_tools.common.clickhouse.config import ClickhouseConfig


def get_config(ctx: Context, try_preprocessed: bool = True) -> ClickhouseConfig:
    if "clickhouse_config" not in ctx.obj:
        ctx.obj["clickhouse_config"] = ClickhouseConfig.load(try_preprocessed)

    return ctx.obj["clickhouse_config"]


def get_macros(ctx):
    return get_config(ctx).macros


def get_cluster_name(ctx):
    return get_config(ctx).cluster_name
