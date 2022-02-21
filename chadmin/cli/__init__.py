from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseConfig


def clickhouse_client(ctx):
    return ctx.obj['chcli']


def execute_query(ctx, query, echo=False, dry_run=False, format='default', **kwargs):
    """
    Execute ClickHouse query.
    """
    if format == 'default':
        format = 'PrettyCompact'

    return clickhouse_client(ctx).query(query, query_args=kwargs, format=format, echo=echo, dry_run=dry_run)


def get_config(ctx):
    if 'config' not in ctx.obj:
        ctx.obj['config'] = ClickhouseConfig.load()

    return ctx.obj['config']


def get_macros(ctx):
    return get_config(ctx).macros


def get_cluster_name(ctx):
    return get_config(ctx).cluster_name
