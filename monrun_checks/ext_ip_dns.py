import click
from cloud.mdb.clickhouse.tools.monrun_checks.result import Result

from functools import cache
import dns.resolver
import requests
import socket
import json

IP_METADATA_PATHS = {
    'public_v4': 'http://169.254.169.254/latest/meta-data/public-ipv4',
    'private_v4': 'http://169.254.169.254/latest/meta-data/local-ipv4',
    'ipv6': 'http://169.254.169.254/latest/meta-data/ipv6',
}


class _TargetRecord:
    def __init__(self, fqdn: str, private: bool, strict: bool):
        self.fqdn = fqdn
        self.private = private
        self.strict = strict


@click.command('ext_ip_dns')
@click.option('--cluster', 'cluster', is_flag=True, help='Check cluster DNS records')
@click.option('--private', 'private', is_flag=True, help='Check private DNS records')
@click.option('--ipv6', 'ipv6', is_flag=True, help='Check AAAA DNS records')
def ext_ip_dns_command(cluster: bool, private: bool, ipv6: bool) -> Result:
    """
    Checks that all DNS records consistent in DC.
    """
    err = []
    for record in _get_host_dns(cluster, private):
        err.extend(_check_fqdn(record, ipv6))

    if not err:
        return Result(0, "OK")

    return Result(2, ', '.join(err))


def _check_fqdn(target: _TargetRecord, ipv6: bool) -> list:
    err = []
    resolver = dns.resolver.get_default_resolver()

    def _compare(record_type: str, ip_type: str) -> bool:
        actual_addr = set(map(lambda a: a.to_text(), resolver.query(target.fqdn, record_type)))
        target_addr = {_get_host_ip(ip_type)}
        if target.strict:
            return target_addr == actual_addr
        return actual_addr.issuperset(target_addr)

    if not _compare('A', 'private_v4' if target.private else 'public_v4'):
        err.append(f'{target.fqdn}: invalid A')
    if ipv6 and not _compare('AAAA', 'ipv6'):
        err.append(f'{target.fqdn}: invalid AAAA')

    return err


@cache
def _get_host_ip(addr_type: str) -> str:
    resp = requests.get(IP_METADATA_PATHS[addr_type])
    resp.raise_for_status()
    return resp.text.strip()


def _get_host_dns(cluster: bool, private: bool) -> list[_TargetRecord]:
    fqdn = socket.getfqdn()
    host_id, cid, suffix = fqdn.split('.', 2)

    result = [_TargetRecord(f'{host_id}.{cid}.{suffix}', private=False, strict=True)]
    if private:
        result += [_TargetRecord(f'{host_id}.{cid}.private.{suffix}', private=True, strict=True)]

    if cluster:
        with open('/etc/dbaas.conf') as f:
            shard = json.load(f)['shard_name']

        result += [
            _TargetRecord(f'rw.{cid}.{suffix}', private=False, strict=False),
            _TargetRecord(f'{shard}.{cid}.{suffix}', private=False, strict=False),
        ]

        if private:
            result += [
                _TargetRecord(f'rw.{cid}.private.{suffix}', private=True, strict=False),
                _TargetRecord(f'{shard}.{cid}.private.{suffix}', private=True, strict=False),
            ]

    return result
