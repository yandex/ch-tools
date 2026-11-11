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


def active_cluster_replicas(
    ctx: Context, cluster: str
) -> dict[int, list[tuple[str, int]]]:
    """Return explicit endpoints only if the named cluster contains retired hosts."""
    retired = decommissioned_hosts(ctx)
    if not retired:
        return {}
    client = clickhouse_client(ctx)
    if "{" in cluster:
        cluster = cluster.format_map(
            dict(
                client.query_json_data("SELECT macro, substitution FROM system.macros")
            )
        )
    name = cluster.replace("\\", "\\\\").replace("'", "\\'")
    rows = client.query_json_data(
        "SELECT shard_num, host_name, port FROM system.clusters "
        f"WHERE cluster = '{name}' ORDER BY shard_num, replica_num"
    )
    if not any(host in retired for _, host, _ in rows):
        return {}
    shards: dict[int, list[tuple[str, int]]] = {}
    for shard, host, port in rows:
        replicas = shards.setdefault(shard, [])
        if host not in retired and (host, port) not in replicas:
            replicas.append((host, port))
    for shard, replicas in shards.items():
        if not replicas:
            raise RuntimeError(
                f"No active replicas remain in shard {shard} of cluster '{cluster}'"
            )
    return shards
