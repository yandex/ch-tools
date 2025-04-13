from datetime import timedelta

from click import Context

from ch_tools.common import logging
from ch_tools.common.clickhouse.client.clickhouse_client import clickhouse_client


def get_uptime(ctx: Context) -> timedelta:
    try:
        return clickhouse_client(ctx).get_uptime()
    except Exception:
        logging.warning("Failed to get ClickHouse uptime", exc_info=True)
        return timedelta()
