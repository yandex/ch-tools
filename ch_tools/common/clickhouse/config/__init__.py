from click import Context

from .clickhouse import ClickhouseConfig
from .clickhouse_keeper import ClickhouseKeeperConfig
from .users import ClickhouseUsersConfig
from .zookeeper import ClickhouseZookeeperConfig

__all__ = [
    "ClickhouseConfig",
    "ClickhouseKeeperConfig",
    "ClickhouseUsersConfig",
    "ClickhouseZookeeperConfig",
]


def get_clickhouse_config(ctx: Context) -> ClickhouseConfig:
    if "clickhouse_config" not in ctx.obj:
        ctx.obj["clickhouse_config"] = ClickhouseConfig.load()

    return ctx.obj["clickhouse_config"]


def get_macros(ctx):
    return get_clickhouse_config(ctx).macros


def get_cluster_name(ctx):
    return get_clickhouse_config(ctx).cluster_name
