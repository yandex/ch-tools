import click
import requests

from ch_tools.common.clickhouse.client.clickhouse_client import clickhouse_client
from ch_tools.common.result import CRIT, OK, WARNING, Result


@click.command("system-metrics")
@click.option("-n", "--name", "name", type=str, help="Metric's name to check")
@click.option("-c", "--critical", "crit", type=int, help="Critical threshold.")
@click.option("-w", "--warning", "warn", type=int, help="Warning threshold.")
@click.pass_context
def system_metrics_command(
    ctx: click.Context,
    name: str,
    crit: int,
    warn: int,
) -> Result:
    """
    Check system metric.
    """
    try:
        metric = _get_metric(ctx, name)
    except IndexError:
        return Result(CRIT, "Metric not available")
    except requests.exceptions.HTTPError as exc:
        return Result(CRIT, repr(exc))

    metric_name, value = metric[0], int(metric[1])
    if value > crit:
        return Result(
            CRIT, f'"{metric_name}" metric\'s value is greater than critical threshold'
        )
    if value > warn:
        return Result(
            WARNING,
            f'"{metric_name}" metric\'s value is greater than warning threshold',
        )

    return Result(OK)


def _get_metric(ctx: click.Context, name: str) -> dict:
    """
    Select and return metric from system.metrics.
    """
    query = "SELECT * from system.metrics WHERE lower(metric) = '{{ name }}'"
    return clickhouse_client(ctx).query_json_data_first_row(
        query=query, query_args={"name": name.lower()}
    )
