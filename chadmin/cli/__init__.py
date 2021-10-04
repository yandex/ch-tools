from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseConfig


def execute_query(ctx, query, echo=False, dry_run=False, format='default', **kwargs):
    if format == 'default':
        format = 'PrettyCompact'

    return ctx.obj['chcli'].query(query, query_args=kwargs, format=format, echo=echo, dry_run=dry_run)


def get_config(ctx):
    if 'config' not in ctx.obj:
        ctx.obj['config'] = ClickhouseConfig.load()

    return ctx.obj['config']


def get_macros(ctx):
    return get_config(ctx).macros


def get_cluster_name(ctx):
    return get_config(ctx).cluster_name
