#!/usr/bin/env python3
import argparse
import socket
import uuid
from kazoo.client import KazooClient
from kazoo.exceptions import ConnectionLoss, ConnectionClosedError, NotEmptyError
from queue import Queue
from threading import Thread


def parse_args():
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--hosts', default=socket.getfqdn() + ':2181', help='ZooKeeper hosts (host:port,host:port,...)')
    parser.add_argument('-s', '--secure', default=False, action='store_true', help='Use secure connection')
    parser.add_argument('--cert', default='', help='Client TLS certificate file')
    parser.add_argument('--key', default='', help='Client TLS key file')
    parser.add_argument('--ca', default='', help='Client TLS ca file')
    parser.add_argument('-u', '--user', default='', help='ZooKeeper ACL user')
    parser.add_argument('-p', '--password', default='', help='ZooKeeper ACL password')
    parser.add_argument('-r', '--root', default='/clickhouse', help='ZooKeeper root path for ClickHouse')
    parser.add_argument('-z', '--zcroot', default='clickhouse/zero_copy', help='ZooKeeper node for new zero-copy data')
    parser.add_argument('--dryrun', default=False, action='store_true', help='Do not perform any actions')
    parser.add_argument('--cleanup', default=False, action='store_true', help='Clean old nodes')
    parser.add_argument('--no-create', default=False, action='store_true', help='Do not crete new nodes')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Verbose mode')
    parser.add_argument('--retries', default=3, type=int, help='Connection retries')
    parser.add_argument('--timeout', default=10, type=int, help='Connection timeout (s)')
    parser.add_argument('--debug', default=False, action='store_true', help='Debug output')
    parser.add_argument('--workers', default=10, type=int, help='Worker threads')
    parser.add_argument(
        '--replicas', default=0, type=int, help='Maximun number of replicas (maximum allowed copies of one part)'
    )

    return parser.parse_args()


# Several folders to heuristic that zookeepr node is folder node
# May be false positive when someone creates set of tables with same paths
table_nodes = ['alter_partition_version', 'block_numbers', 'blocks', 'columns', 'leader_election']
zc_nodes = ['zero_copy_s3', 'zero_copy_hdfs']


class Worker(Thread):
    def __init__(self, args, queue, number):
        Thread.__init__(self)
        self.client = get_client(args)
        self.args = args
        self.queue = queue
        self.number = number

    def run(self):
        while True:
            part_path, new_part_path = self.queue.get()
            retry = 0
            while retry < self.args.retries:
                try:
                    retry += 1
                    convert_part(self.client, self.args, part_path, new_part_path)
                    break
                except (ConnectionLoss, ConnectionClosedError):
                    if retry < self.args.retries:
                        if self.args.debug:
                            print(f'Worker {self.number} lost connection. Reconnect...')
                        self.client.stop()
                        self.client = get_client(self.args)
                    else:
                        print(f'Worker {self.number} lost connection. Retries are over.')
                        raise
            self.queue.task_done()


def convert_part(client, args, part_path, new_part_path):
    uniq_ids = client.get_children(part_path)
    if args.debug:
        print(f'In part "{part_path}" found {len(uniq_ids)} uniq ids')
    if args.replicas > 0 and len(uniq_ids) > args.replicas:
        print(f'In part "{part_path}" found too much uniq ids: {len(uniq_ids)}, skipped`')
    else:
        for uniq_id in uniq_ids:
            uniq_path = f'{part_path}/{uniq_id}'
            replicas = client.get_children(uniq_path)
            if args.debug:
                print(f'In uniq_id "{uniq_path}" found {len(replicas)} replicas')
            for replica in replicas:
                replica_path = f'{uniq_path}/{replica}'
                new_path = f'{new_part_path}/{uniq_id}/{replica}'
                if not args.no_create:
                    if not client.exists(new_path):
                        if args.verbose:
                            print(f'Make node "{new_path}"')
                        if not args.dryrun:
                            client.ensure_path(f'{new_part_path}/{uniq_id}')
                            client.create(new_path, value=b'lock')
                    elif args.debug:
                        print(f'Path {new_path} already exists')
                if args.cleanup:
                    if args.verbose:
                        print(f'Remove node "{replica_path}"')
                    if not args.dryrun:
                        client.delete(replica_path)
            if args.cleanup and not args.dryrun:
                client.delete(uniq_path)
    if args.cleanup and not args.dryrun:
        client.delete(part_path, recursive=True)


def get_children(client, args, path):
    retry = 0
    nodes = []
    while retry < args.retries:
        try:
            retry += 1
            nodes = client.get_children(path)
            break
        except (ConnectionLoss, ConnectionClosedError):
            if retry < args.retries:
                if args.debug:
                    print('Main thread lost connection. Reconnect...')
                client.stop()
                client = get_client(args)
            else:
                print('Main thread lost connection. Retries are over.')
                raise
    return nodes


def convert_node(queue, client, args, path, zc_node):
    base_path = f'{path}/{zc_node}/shared'
    parts = client.get_children(base_path)
    if args.debug:
        print(f'In table "{path}" node "{zc_node}" found {len(parts)} parts')
    table_id_path = f'{path}/table_shared_id'
    table_id = ''
    if client.exists(table_id_path):
        table_id = client.get(table_id_path)[0].decode('UTF-8')
    else:
        table_id = str(uuid.uuid4())
        if args.verbose:
            print(f'Make table_id "{table_id_path}" = "{table_id}"')
        if not args.dryrun:
            client.create(table_id_path, bytes(table_id, 'UTF-8'))
    if args.debug:
        print(f'For path "{path}" found table_id "{table_id}"')
    for part in parts:
        part_path = f'{base_path}/{part}'
        new_part_path = f'{args.root}/{args.zcroot}/{zc_node}/{table_id}/{part}'
        queue.put((part_path, new_part_path))
    if args.cleanup and not args.dryrun:
        try:
            client.delete(base_path)
            client.delete(f'{path}/{zc_node}')
        except NotEmptyError:
            # When other replicas still in compatibility mode they can create subnodes
            # Node will be deleted later from that replica
            if args.debug:
                print(f'Other replica create subnode in "{path}/{zc_node}", skip delete')


def convert_table(queue, client, args, path, nodes):
    print(f'Convert table nodes by path "{path}"')
    for zc_node in zc_nodes:
        if zc_node in nodes:
            retry = 0
            while retry < args.retries:
                try:
                    retry += 1
                    convert_node(queue, client, args, path, zc_node)
                    break
                except (ConnectionLoss, ConnectionClosedError):
                    if retry < args.retries:
                        if args.debug:
                            print('Main thread lost connection. Reconnect...')
                        client.stop()
                        client = get_client(args)
                    else:
                        print('Main thread lost connection. Retries are over.')
                        raise
    if args.debug:
        print(f'Convert table nodes by path "{path}" completed')


def is_like_a_table(nodes):
    for tn in table_nodes:
        if tn not in nodes:
            return False
    return True


def scan_recursive(queue, client, args, path):
    if path == f'{args.root}/{args.zcroot}':
        if args.debug:
            print(f'Skip scan node "{path}"')
        return
    if args.debug:
        print(f'Scan node "{path}"')
    nodes = get_children(client, args, path)
    if is_like_a_table(nodes):
        convert_table(queue, client, args, path, nodes)
    else:
        for node in nodes:
            scan_recursive(queue, client, args, f'{path}/{node}')


def scan(queue, client, args):
    nodes = get_children(client, args, args.root)
    for node in nodes:
        if node != args.zcroot:
            scan_recursive(queue, client, args, f'{args.root}/{node}')


def get_client(args):
    client = KazooClient(
        connection_retry=args.retries,
        command_retry=args.retries,
        timeout=args.timeout,
        hosts=args.hosts,
        use_ssl=args.secure,
        certfile=args.cert,
        keyfile=args.key,
        ca=args.ca,
    )
    client.start()
    if args.user and args.password:
        client.add_auth('digest', f'{args.user}:{args.password}')
    return client


def main():
    args = parse_args()
    client = get_client(args)

    queue = Queue()
    workers = []
    for x in range(args.workers):
        workers.append(Worker(args, queue, x))
        workers[x].daemon = True
        workers[x].start()

    scan(queue, client, args)

    queue.join()


if __name__ == '__main__':
    main()
