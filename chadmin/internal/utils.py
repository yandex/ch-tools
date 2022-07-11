"""
Utility functions.
"""


def clickhouse_client(ctx):
    """
    Return ClickHouse client from the context.
    """
    return ctx.obj['chcli']


def execute_query(ctx, query, timeout=None, echo=False, dry_run=False, format='default', **kwargs):
    """
    Execute ClickHouse query.
    """
    if format == 'default':
        format = 'PrettyCompact'

    return clickhouse_client(ctx).query(
        query=query, query_args=kwargs, timeout=timeout, format=format, echo=echo, dry_run=dry_run
    )
