"""Select a fixed set of replicas for an object-storage collection run."""

from dataclasses import dataclass
from typing import Optional

from click import Context

from ch_tools.chadmin.internal.utils import (
    execute_query,
    get_unavailable_replicas,
    replace_macros,
)
from ch_tools.common import logging
from ch_tools.monrun_checks.clickhouse_info import ClickhouseInfo


@dataclass(frozen=True)
class ObjectStorageReplicas:
    hosts: list[str]
    excluded: list[str]


def get_object_storage_replicas(
    ctx: Context, replicas: Optional[list[str]] = None
) -> ObjectStorageReplicas:
    """Allow missing replicas only when their exact names are configured."""
    allowed_hosts = _get_allowed_unavailable_hosts(ctx)
    if replicas is None:
        replicas = ClickhouseInfo.get_replicas(ctx)
    if not replicas:
        raise RuntimeError("No replicas found for the current shard")
    unavailable = get_unavailable_replicas(ctx, replicas=replicas)
    _check_unavailable_hosts(unavailable, allowed_hosts)
    hosts = [host for host in replicas if host not in unavailable]
    if not hosts:
        raise RuntimeError(
            "No available replicas remain for the object-storage operation"
        )
    return ObjectStorageReplicas(hosts, unavailable)


def _get_allowed_unavailable_hosts(ctx: Context) -> list[str]:
    allowed_hosts = ctx.obj["config"]["object_storage"].get("unavailable_hosts", [])
    if not isinstance(allowed_hosts, list) or any(
        not isinstance(host, str) or not host.strip() for host in allowed_hosts
    ):
        raise ValueError(
            "object_storage.unavailable_hosts must be a list of nonempty strings"
        )
    return allowed_hosts


def _check_unavailable_hosts(unavailable: list[str], allowed_hosts: list[str]) -> None:
    unexpected = [host for host in unavailable if host not in allowed_hosts]
    if unexpected:
        raise RuntimeError(
            "Not all replicas are available. Unavailable replicas without an "
            f"exemption: {', '.join(unexpected)}."
        )
    for host in unavailable:
        logging.warning(
            "Excluding unavailable replica {}: listed in object_storage.unavailable_hosts",
            host,
        )


def get_object_storage_cluster_replicas(
    ctx: Context, cluster: str
) -> dict[int, dict[int, str]]:
    """Get configured replicas in clusterAllReplicas shard order."""
    if "{" in cluster:
        macros = dict(
            execute_query(
                ctx,
                "SELECT macro, substitution FROM system.macros",
                format_="JSONCompact",
            )["data"]
        )
        cluster = replace_macros(cluster, macros)
    escaped_cluster = cluster.replace("\\", "\\\\").replace("'", "\\'")
    rows = execute_query(
        ctx,
        "SELECT shard_num, host_name, port FROM system.clusters "
        f"WHERE cluster = '{escaped_cluster}' ORDER BY shard_num, replica_num",
        format_="JSONCompact",
    )["data"]
    if not rows:
        raise RuntimeError(f"No replicas found for cluster '{cluster}'")
    shards: dict[int, dict[int, str]] = {}
    replica_numbers: dict[tuple[str, int], int] = {}
    for shard, host, port in rows:
        # clusterAllReplicas deduplicates endpoints before assigning _shard_num.
        replica_num = replica_numbers.setdefault((host, port), len(replica_numbers) + 1)
        shards.setdefault(shard, {})[replica_num] = host
    return shards


def check_object_storage_cluster_replicas(
    ctx: Context,
    cluster: str,
    shards: dict[int, dict[int, str]],
    responding_replicas: set[int],
) -> list[int]:
    """Validate actual query participants and select one response per shard."""
    replicas = {num: host for hosts in shards.values() for num, host in hosts.items()}
    _check_unavailable_hosts(
        [host for num, host in replicas.items() if num not in responding_replicas],
        _get_allowed_unavailable_hosts(ctx),
    )
    selected = []
    for shard, hosts in shards.items():
        available = responding_replicas.intersection(hosts)
        if not available:
            raise RuntimeError(
                f"No available replicas remain in shard {shard} of cluster '{cluster}'"
            )
        selected.append(min(available))
    return selected
