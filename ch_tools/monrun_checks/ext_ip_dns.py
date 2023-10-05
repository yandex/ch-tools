import json
import socket
from functools import lru_cache
from typing import List, Tuple

import click
import dns.resolver
import requests

from ch_tools.common.result import Result

IP_METADATA_PATHS = {
    "public_v4": "http://169.254.169.254/latest/meta-data/public-ipv4",
    "private_v4": "http://169.254.169.254/latest/meta-data/local-ipv4",
    "ipv6": "http://169.254.169.254/latest/meta-data/ipv6",
}

IP_METADATA_PATHS_GCP = {
    "public_v4": "http://169.254.169.254/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip",
    "private_v4": "http://169.254.169.254/computeMetadata/v1/instance/network-interfaces/0/ip",
    "ipv6": "http://169.254.169.254/computeMetadata/v1/instance/network-interfaces/0/ipv6",
}


class _TargetRecord:
    def __init__(self, fqdn: str, private: bool, strict: bool):
        self.fqdn = fqdn
        self.private = private
        self.strict = strict


@click.command("ext_ip_dns")
@click.option("--cluster", "cluster", is_flag=True, help="Check cluster DNS records")
@click.option("--private", "private", is_flag=True, help="Check private DNS records")
@click.option("--ipv6", "ipv6", is_flag=True, help="Check AAAA DNS records")
@click.option(
    "--imdsv2", "imdsv2", is_flag=True, help="Use imdsv2 token for non gcp hosts"
)
def ext_ip_dns_command(
    cluster: bool, private: bool, ipv6: bool, imdsv2: bool
) -> Result:
    """
    Check that all DNS records consistent.
    """
    err = []
    for record in _get_host_dns(cluster, private):
        err.extend(_check_fqdn(record, ipv6, imdsv2))

    if not err:
        return Result(0, "OK")

    return Result(2, ", ".join(err))


def _check_fqdn(target: _TargetRecord, ipv6: bool, imdsv2: bool) -> list:
    err = []
    resolver = dns.resolver.Resolver()

    def _compare(record_type: str, ip_type: str) -> Tuple[bool, set, set]:
        try:
            actual_addr = set(
                map(lambda a: a.to_text(), resolver.resolve(target.fqdn, record_type))
            )
        except dns.resolver.NXDOMAIN:
            actual_addr = set()
        target_addr = {_get_host_ip(ip_type, imdsv2)}
        if target.strict:
            return target_addr == actual_addr, target_addr, actual_addr
        return actual_addr.issuperset(target_addr), target_addr, actual_addr

    ok, target_addr, actual_addr = _compare(
        "A", "private_v4" if target.private else "public_v4"
    )
    if not ok:
        err.append(
            f"{target.fqdn}: invalid A: expected {target_addr}, actual {actual_addr}"
        )
    ok, target_addr, actual_addr = _compare("AAAA", "ipv6")
    if ipv6 and not ok:
        err.append(
            f"{target.fqdn}: invalid AAAA: expected {target_addr}, actual {actual_addr}"
        )

    return err


@lru_cache(maxsize=None)
def _get_host_ip(addr_type: str, imdsv2: bool) -> str:
    # pylint: disable=missing-timeout
    if _is_gcp():
        resp = requests.get(
            IP_METADATA_PATHS_GCP[addr_type], headers={"Metadata-Flavor": "Google"}
        )
    else:
        headers = {}
        if imdsv2:
            headers["X-aws-ec2-metadata-token"] = _get_imdsv2_token()
        resp = requests.get(IP_METADATA_PATHS[addr_type], headers=headers)
    resp.raise_for_status()
    return resp.text.strip()


@lru_cache(maxsize=None)
def _get_imdsv2_token():
    return requests.put(
        "http://169.254.169.254/latest/api/token",
        headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
        timeout=10,
    ).text


@lru_cache(maxsize=None)
def _is_gcp():
    with open("/etc/dbaas.conf", encoding="utf-8") as f:
        vtype = json.load(f).get("flavor", {}).get("vtype", "")
        return vtype == "gcp"


def _get_host_dns(cluster: bool, private: bool) -> List[_TargetRecord]:
    fqdn = socket.getfqdn()
    host_id, cid, suffix = fqdn.split(".", 2)

    result = [_TargetRecord(f"{host_id}.{cid}.{suffix}", private=False, strict=True)]
    if private:
        result += [
            _TargetRecord(
                f"{host_id}.{cid}.private.{suffix}", private=True, strict=True
            )
        ]

    if cluster:
        with open("/etc/dbaas.conf", encoding="utf-8") as f:
            shard = json.load(f)["shard_name"]

        result += [
            _TargetRecord(f"rw.{cid}.{suffix}", private=False, strict=False),
            _TargetRecord(f"{shard}.{cid}.{suffix}", private=False, strict=False),
        ]

        if private:
            result += [
                _TargetRecord(f"rw.{cid}.private.{suffix}", private=True, strict=False),
                _TargetRecord(
                    f"{shard}.{cid}.private.{suffix}", private=True, strict=False
                ),
            ]

    return result
