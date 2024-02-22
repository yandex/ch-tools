from ch_tools.common.clickhouse.client.clickhouse_client import (
    ClickhousePort,
    clickhouse_client,
)


def execute_query_client(
    ch_client,
    query,
    timeout=None,
    echo=False,
    dry_run=False,
    compact=True,
    stream=False,
    settings=None,
    port=ClickhousePort.AUTO,
    **kwargs
):
    """
    Execute ClickHouse query using given client.
    """
    format_ = "JSON"
    if compact:
        format_ = "JSONCompact"

    return ch_client.query(
        query=query,
        query_args=kwargs,
        timeout=timeout,
        format_=format_,
        echo=echo,
        dry_run=dry_run,
        stream=stream,
        settings=settings,
        port=port,
    )["data"]


def execute_query(
    ctx,
    query,
    timeout=None,
    echo=False,
    dry_run=False,
    compact=True,
    stream=False,
    settings=None,
    port=ClickhousePort.AUTO,
    **kwargs
):
    """
    Execute ClickHouse query.
    """

    return execute_query_client(
        clickhouse_client(ctx),
        query,
        timeout,
        echo,
        dry_run,
        compact,
        stream,
        settings,
        port,
        **kwargs,
    )
