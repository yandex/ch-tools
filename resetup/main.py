#!/usr/bin/env python3
import argparse
import grp
import json
import logging
import os
import pwd
import re
import socket
import subprocess
from time import sleep

import requests
import tenacity
from kazoo.client import KazooClient

CLIENT_RETRIES = dict(max_tries=6,
                      delay=0.1,
                      backoff=2,
                      max_jitter=0.8,
                      max_delay=60)

GET_TABLES_SQL = """
    SELECT
        t.database,
        t.name,
        t.metadata_path,
        t.create_table_query,
        r.zookeeper_path
    FROM system.tables t
    LEFT JOIN system.replicas r ON t.database = r.database
        AND t.name = r.table
        AND r.replica_name = '{replica_name}'
    WHERE t.database NOT IN ('system')
        AND (empty({tables}) OR has(cast({tables}, 'Array(String)'), t.name))
    ORDER BY metadata_modification_time
    FORMAT JSON
"""

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

    return parser.parse_args()


def get_dbaas_conf():
    """
    Load /etc/dbaas.conf.
    """
    with open('/etc/dbaas.conf', 'r') as fd:
        return json.load(fd)


def dump_metadata(metadata):
    """
    Dump DDL locally.
    """

    def change_owner(path):
        uid = pwd.getpwnam('clickhouse').pw_uid
        gid = grp.getgrnam('clickhouse').gr_gid
        os.chown(path, uid, gid)

    def dump_ddl(ddl, path):
        logging.debug('Dumping metadata at path: %s', path)
        db_dir = os.path.dirname(path)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
            change_owner(db_dir)
        with open(path, 'w') as fd:
            fd.write(ddl)
        change_owner(path)

    for tb in metadata:
        dump_ddl(tb['ddl'], tb['metadata_path'])


def get_ddl_metadata(ch_client, src_host, tables_to_restore):
    """
    Fetch DDL metadata from source replica.
    """

    logging.debug('Getting DDL metadata from CH host: %s', src_host)

    data = ch_client.query(
        GET_TABLES_SQL.format(tables=tables_to_restore, replica_name=src_host))

    result = []
    for table in data['data']:
        result.append({
            'name': table['name'],
            'ddl': re.sub('^CREATE', 'ATTACH', table['create_table_query']),
            'zookeeper_path': table['zookeeper_path'],
            'metadata_path': table['metadata_path']
        })
    return result


def check_no_user_tables(ch_client):
    """
    Check restoring replica doesn't have user tables.
    """
    logging.debug('Checking restoring replica has no user tables')
    result = ch_client.query(GET_USER_TABLES_SQL)
    if result['data']:
        raise RuntimeError('Restoring replica should have no user tables')


def check_zookeeper_has_metadata(zk_hosts, cid, target_host, metadata):
    """
    Check that there is all required metadata for target replica in Zookeeper.
    """
    result = True
    logging.debug('Checking Zookeeper has required metadata for tables')

    zk_client = KazooClient(hosts=','.join(zk_hosts),
                            connection_retry=CLIENT_RETRIES,
                            command_retry=CLIENT_RETRIES,
                            timeout=1.0)
    zk_client.start()
    for tb in metadata:
        # interpret tables without zookeeper path as views or
        # distributes tables
        if not tb['zookeeper_path']:
            continue
        path = os.path.join('/clickhouse', cid, tb['zookeeper_path'], 'replicas', target_host)
        if not zk_client.exists(path):
            result = False
            logging.debug('No metadata in ZK for table %s', tb['name'])
    zk_client.stop()
    return result


def clean_zookeeper_tables_metadata(zk_hosts, cid, target_shard_name, target_host):
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

    path = os.path.join('/clickhouse', cid, 'clickhouse', 'tables', target_shard_name)
    logging.debug('Start cleaning ZK metadata from path: %s', path)
    client = KazooClient(hosts=','.join(zk_hosts),
                         connection_retry=CLIENT_RETRIES,
                         command_retry=CLIENT_RETRIES,
                         timeout=1.0)
    client.start()
    client.ensure_path(path)
    rec_node_delete(client, path, target_host)
    client.stop()


def set_restore_flag():
    """
    Set ClickHouse restore flag. It's needed to run internal restoring process after ClickHouse restart.
    """
    logging.debug('Setting ClickHouse restore flag')
    cmd = [
        'sudo', '-u', 'clickhouse', 'touch',
        '/var/lib/clickhouse/flags/force_restore_data'
    ]
    subprocess.check_call(cmd, shell=False)


def restart_clickhouse(service_manager):
    """
    Restart local ClickHouse instance.
    """
    logging.debug('Restarting ClickHouse')
    if service_manager == 'supervisord':
        cmd = ['supervisorctl', 'restart', 'clickhouse-server']
    else:
        cmd = ['service', 'clickhouse-server', 'restart']
    subprocess.check_call(cmd, shell=False, timeout=5 * 60)


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


def delete_backup(backup_id):
    """
    Delete given ClickHouse backup.
    """
    cmd = ['ch-backup', 'delete', backup_id]

    run_ch_backup_command(cmd)


def get_backup(src_host):
    """
    Create ClickHouse schema-only backup on src_host.
    """
    cmd = ['ch-backup', '--host', src_host, 'backup', '--force', '--schema-only']

    backup_id = run_ch_backup_command(cmd).decode().strip()

    return backup_id


def restore_schema(src_host):
    """
    Make a schema backup on src_host and restore this schema locally.
    """
    backup_id = get_backup(src_host)

    cmd = ['ch-backup', 'restore', backup_id, '--schema-only']

    try:
        return run_ch_backup_command(cmd)
    finally:
        delete_backup(backup_id)


def run_ch_backup_command(cmd):
    """
    ClickHouse backup command runner wrapper.
    """
    logging.debug(f'Running: {cmd}')

    # Workaround if locale is not set.
    env = None
    if not os.environ.get('LC_ALL'):
        env = {'LC_ALL': 'en_US.UTF-8'}

    try:
        output = subprocess.check_output(cmd, env=env, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
        logging.exception(f'{cmd} failed: {exc.output} {exc.returncode}')
        raise
    else:
        return output


def main():
    args = parse_args()

    logging.getLogger('kazoo').setLevel('WARNING')
    if args.verbose:
        logging.basicConfig(level='DEBUG', format='%(message)s')

    tables_to_restore = [t for t in args.tables.split(',') if t]
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
    ddl_metadata = get_ddl_metadata(ClickhouseClient(src_host, args.insecure), src_host, tables_to_restore)
    if check_zookeeper_has_metadata(zk_hosts, cid, target_host, ddl_metadata):
        logging.debug('ZK has all required metadata. Dumping DDL and restore ClickHouse locally.')
        dump_metadata(ddl_metadata)
        set_restore_flag()
    else:
        logging.debug('Cleaning ZK metadata and restore schema from backup')
        clean_zookeeper_tables_metadata(zk_hosts, cid, shard_name, target_host)
        restore_schema(src_host)

    restart_clickhouse(args.service_manager)

    # TODO: wait until data is copied from a replica
    sleep(5)


if __name__ == '__main__':
    main()
