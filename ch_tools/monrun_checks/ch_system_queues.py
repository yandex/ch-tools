from typing import Any

from cloup import command, option, pass_context

from ch_tools.common.clickhouse.client.clickhouse_client import clickhouse_client
from ch_tools.common.result import CRIT, OK, WARNING, Result


@command("system-queues")
@option("--merges-in-queue-warn", "merges_in_queue_warn", type=int)
@option("--merges-in-queue-crit", "merges_in_queue_crit", type=int)
@option("--future-parts-warn", "future_parts_warn", type=int)
@option("--future-parts-crit", "future_parts_crit", type=int)
@option("--parts-to-check-warn", "parts_to_check_warn", type=int)
@option("--parts-to-check-crit", "parts_to_check_crit", type=int)
@option("--queue-size-warn", "queue_size_warn", type=int)
@option("--queue-size-crit", "queue_size_crit", type=int)
@option("--inserts-in-queue-warn", "inserts_in_queue_warn", type=int)
@option("--inserts-in-queue-crit", "inserts_in_queue_crit", type=int)
@pass_context
def system_queues_command(
    ctx: Any,
    merges_in_queue_warn: int,
    merges_in_queue_crit: int,
    future_parts_warn: int,
    future_parts_crit: int,
    parts_to_check_warn: int,
    parts_to_check_crit: int,
    queue_size_warn: int,
    queue_size_crit: int,
    inserts_in_queue_warn: int,
    inserts_in_queue_crit: int,
) -> Result:
    """
    Check system queues.
    """
    thresholds = [
        ("merges_in_queue", merges_in_queue_warn, merges_in_queue_crit),
        ("future_parts", future_parts_warn, future_parts_crit),
        ("parts_to_check", parts_to_check_warn, parts_to_check_crit),
        ("queue_size", queue_size_warn, queue_size_crit),
        ("inserts_in_queue", inserts_in_queue_warn, inserts_in_queue_crit),
    ]

    issues = []
    for item in _get_metrics(ctx):
        table_full_name = f"{item['database']}.{item['table']}"
        for parameter, warn, crit in thresholds:
            value = item[parameter]
            if value > crit:
                issues.append(
                    (
                        CRIT,
                        f"{table_full_name}: {parameter} {value} > {crit} (crit);",
                    )
                )
            elif value > warn:
                issues.append(
                    (
                        WARNING,
                        f"{table_full_name}: {parameter} {value} > {warn} (warn);",
                    )
                )

    if issues:
        issues.sort(reverse=True, key=lambda x: x[0])
        status = issues[0][0]
        message = " ".join(x[1] for x in issues)
        return Result(status, message)

    return Result(OK)


def _get_metrics(ctx: Any) -> list[dict]:
    """
    Select and return metrics form system.replicas.
    """
    query = (
        "SELECT database, table, future_parts, parts_to_check, queue_size,"
        " inserts_in_queue, merges_in_queue FROM system.replicas"
    )
    return clickhouse_client(ctx).query_json_data(query=query, compact=False)
