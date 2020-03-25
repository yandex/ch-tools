#!/usr/bin/env python3
import argparse
import json
import logging
import os
import socket
import subprocess

import requests
import tenacity
from kazoo.client import KazooClient

CLIENT_RETRIES = dict(max_tries=6,
                      delay=0.1,
                      backoff=2,
                      max_jitter=0.8,
                      max_delay=60)

GET_USER_TABLES_SQL = """
    SELECT name
    FROM system.tables
    WHERE database <> 'system'
    FORMAT JSON
"""


def retry(exception_types, max_attempts=5, max_interval=5):
    """
    Function decorator that retries wrapped function on failures.
    """
    return tenacity.retry(
        retry=tenacity.retry_if_exception_type(exception_types),
        wait=tenacity.wait_random_exponential(multiplier=0.5,
                                              max=max_interval),
        stop=tenacity.stop_after_attempt(max_attempts),
        reraise=True)


class ClickhouseClient:
    """
    ClickHouse client.
    """

    def __init__(self, host, insecure=False):
        self._session = requests.Session()
        if insecure:
            self._session.verify = False
        else:
            self._session.verify = '/etc/clickhouse-server/ssl/allCAs.pem'
        self._url = f'https://{host}:8443'
        self._timeout = 60

    @retry(requests.exceptions.ConnectionError)
    def query(self, query, post_data=None):
        """
        Execute query.
        """
        logging.debug('Executing CH query: %s', query)
        response = self._session.post(self._url,
                                      params={
                                          'query': query,
                                      },
                                      json=post_data,
                                      timeout=self._timeout)

        response.raise_for_status()
        return response.json()


def parse_args():
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default=socket.getfqdn(), help='')
    parser.add_argument('--tables',
                        default='',
                        help='Comma-separated list of tables to restore')
    parser.add_argument('--service-manager',
                        choices=['systemd', 'supervisord'],
                        default='systemd')
    parser.add_argument('--insecure',
                        action='store_true',
                        default=False,
                        help='Disable certificate verification for ClickHouse requests.')
    parser.add_argument('-v',
                        '--verbose',
                        action='store_true',
                        default=False,
                        help='Verbose mode.')
    parser.add_argument('--zk-root', default='/clickhouse', help='zk path to clean metadata')

    return parser.parse_args()


def get_dbaas_conf():
    """
    Load /etc/dbaas.conf.
    """
    with open('/etc/dbaas.conf', 'r') as fd:
        return json.load(fd)


def check_no_user_tables(ch_client):
    """
    Check restoring replica doesn't have user tables.
    """
    logging.debug('Checking restoring replica has no user tables')
    result = ch_client.query(GET_USER_TABLES_SQL)
    if result['data']:
        raise RuntimeError('Restoring replica should have no user tables')


def clean_zookeeper_tables_metadata(zk_hosts, zk_root, target_host):
    """
    Recursively delete all ZK nodes belongs to target_host.
    """

    def rec_node_delete(zk_client, path, node):
        for subpath in zk_client.get_children(path):
            if subpath == node:
                logging.debug('Deleting ZK path: %s', os.path.join(path, subpath))
                zk_client.delete(os.path.join(path, subpath), recursive=True)
            else:
                rec_node_delete(zk_client, os.path.join(path, subpath), node)

    logging.debug('Cleaning ZK metadata and restore schema from backup')
    logging.debug(f'Start cleaning ZK metadata from path: {zk_root}')
    client = KazooClient(hosts=','.join(zk_hosts),
                         connection_retry=CLIENT_RETRIES,
                         command_retry=CLIENT_RETRIES,
                         timeout=1.0)
    client.start()
    rec_node_delete(client, zk_root, target_host)
    client.stop()


def get_ch_shard_hosts_and_zk_hosts(conf, target_host):
    """
    Get shard of target host, all hosts belong to this shard and Zookeeper hosts.
    """
    zk_hosts = []
    ch_shard_hosts = []
    shard_name = None
    for subcid, sub_conf in conf['cluster']['subclusters'].items():
        if 'zk' in sub_conf['roles']:
            zk_hosts.extend(sub_conf['hosts'])
        if 'clickhouse_cluster' in sub_conf['roles']:
            for shard_id, sh_conf in sub_conf['shards'].items():
                if target_host in sh_conf['hosts']:
                    ch_shard_hosts.extend(sh_conf['hosts'])
                    shard_name = sh_conf['name']
                    break
    return shard_name, ch_shard_hosts, zk_hosts


def restore_schema(src_host):
    """
    Make a schema backup on src_host and restore this schema locally.
    """

    cmd = ['ch-backup', 'restore-schema', '--source-host', src_host, '--source-port', '8443']
    logging.debug(f'Running: {cmd}')

    # Workaround if locale is not set.
    env = None
    if not os.environ.get('LC_ALL'):
        env = {'LC_ALL': 'en_US.UTF-8'}

    try:
        output = subprocess.check_output(cmd, env=env, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
        output = str(exc.output).replace('\\n', '\n    ')
        logging.exception(f'{cmd} failed: {output} {exc.returncode}')
        raise
    else:
        return output


def main():
    args = parse_args()

    logging.getLogger('kazoo').setLevel('WARNING')
    if args.verbose:
        logging.basicConfig(level='DEBUG', format='%(message)s')

    target_host = args.host
    conf = get_dbaas_conf()

    cid = conf['cluster_id']
    shard_name, ch_shard_hosts, zk_hosts = get_ch_shard_hosts_and_zk_hosts(conf, target_host)
    if not zk_hosts:
        raise RuntimeError('Zookeeper hosts are not found')
    if not ch_shard_hosts:
        raise RuntimeError('Target host is not found among ClickHouse shards')

    logging.debug('Cluster id: %s', cid)
    logging.debug('Recovering host: %s', target_host)
    logging.debug('Zookeeper hosts: %s', zk_hosts)
    logging.debug('ClickHouse shard hosts: %s', ch_shard_hosts)

    # Select a host to take metadata from
    for h in ch_shard_hosts:
        if h != target_host:
            src_host = h
            break
    else:
        raise RuntimeError('No source host available')

    check_no_user_tables(ClickhouseClient(target_host, args.insecure))
    clean_zookeeper_tables_metadata(zk_hosts, args.zk_root, target_host)
    restore_schema(src_host)


if __name__ == '__main__':
    main()
