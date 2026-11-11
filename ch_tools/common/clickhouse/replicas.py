"""Select ClickHouse hosts that are still in service."""

from functools import lru_cache

from click import Context

from ch_tools.common.clickhouse.client.clickhouse_client import clickhouse_client
from ch_tools.monrun_checks.clickhouse_info import ClickhouseInfo


def decommissioned_hosts(ctx: Context) -> set[str]:
    hosts = ctx.obj["config"]["clickhouse"].get("decommissioned_hosts", [])
    if not isinstance(hosts, list) or any(
        not isinstance(h, str) or not h.strip() for h in hosts
    ):
        raise ValueError("clickhouse.decommissioned_hosts must be a list of host names")
    return set(hosts)


@lru_cache(maxsize=1)
def active_shard_hosts(ctx: Context) -> list[str]:
    hosts = ClickhouseInfo.get_replicas(ctx)
    retired = decommissioned_hosts(ctx)
    active = [host for host in hosts if host not in retired]
    if not active:
        raise RuntimeError("No active replicas remain in the current shard")
    return active


def shard_has_decommissioned_hosts(ctx: Context) -> bool:
    return len(active_shard_hosts(ctx)) != len(ClickhouseInfo.get_replicas(ctx))


def check_cluster_for_decommissioned_hosts(ctx: Context, cluster: str) -> None:
    """Check the named cluster locally before any distributed space-usage query."""
    retired = decommissioned_hosts(ctx)
    if not retired:
        return
    client = clickhouse_client(ctx)
    if "{" in cluster:
        macros = client.query_json_data("SELECT macro, substitution FROM system.macros")
        cluster = cluster.format_map(dict(macros))
    name = cluster.replace("\\", "\\\\").replace("'", "\\'")
    rows = client.query_json_data(
        f"SELECT DISTINCT host_name FROM system.clusters WHERE cluster = '{name}'"
    )
    if not rows:
        raise RuntimeError(f"No replicas found for cluster '{cluster}'")
    excluded = retired.intersection(host for (host,) in rows)
    if excluded:
        raise RuntimeError(
            f"Cluster '{cluster}' contains decommissioned hosts: {', '.join(sorted(excluded))}. "
            "Use --cluster with only active replicas or --scope shard for local statistics."
        )
