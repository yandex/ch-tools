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
    allowed_hosts = ctx.obj["config"]["object_storage"].get("unavailable_hosts", [])
    if not isinstance(allowed_hosts, list) or any(
        not isinstance(host, str) or not host.strip() for host in allowed_hosts
    ):
        raise ValueError(
            "object_storage.unavailable_hosts must be a list of nonempty strings"
        )

    if replicas is None:
        replicas = ClickhouseInfo.get_replicas(ctx)
    if not replicas:
        raise RuntimeError("No replicas found for the current shard")
    unavailable = get_unavailable_replicas(ctx, replicas=replicas)
    unexpected = [host for host in unavailable if host not in allowed_hosts]
    if unexpected:
        raise RuntimeError(
            "Not all replicas are available. Unavailable replicas without an "
            f"exemption: {', '.join(unexpected)}."
        )
    hosts = [host for host in replicas if host not in unavailable]
    if not hosts:
        raise RuntimeError(
            "No available replicas remain for the object-storage operation"
        )
    for host in unavailable:
        logging.warning(
            "Excluding unavailable replica {}: listed in object_storage.unavailable_hosts",
            host,
        )
    return ObjectStorageReplicas(hosts, unavailable)


def get_object_storage_cluster_replicas(
    ctx: Context, cluster: str
) -> dict[int, list[str]]:
    """Select available replicas, requiring at least one in every cluster shard."""
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
        "SELECT shard_num, host_name FROM system.clusters "
        f"WHERE cluster = '{escaped_cluster}' ORDER BY shard_num, replica_num",
        format_="JSONCompact",
    )["data"]
    if not rows:
        raise RuntimeError(f"No replicas found for cluster '{cluster}'")
    selected = get_object_storage_replicas(
        ctx, replicas=list(dict.fromkeys(host for _, host in rows))
    )
    shards: dict[int, list[str]] = {}
    for shard, host in rows:
        hosts = shards.setdefault(shard, [])
        if host in selected.hosts:
            hosts.append(host)
    for shard, hosts in shards.items():
        if not hosts:
            raise RuntimeError(
                f"No available replicas remain in shard {shard} of cluster '{cluster}'"
            )
    return shards
