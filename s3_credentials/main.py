#!/usr/bin/env python3
"""
Manage default ClickHouse s3 credentials.
"""

import argparse
import json
import requests
import time
import random
from datetime import timedelta
from os.path import getmtime
from xml.dom import minidom


def _parse_args():
    parser = argparse.ArgumentParser()
    actions = parser.add_subparsers()

    update = actions.add_parser('update', help='update ch default s3 credentials config')
    update.set_defaults(func=_update_config)
    update.add_argument('-e', '--endpoint', type=str, help='S3 endpoint')
    update.add_argument('-m', '--metadata-address', type=str, default='169.254.169.254',
                        help='compute metadata api address')
    update.add_argument('-s', '--random-sleep', action='store_true', default=False,
                        help='whether need a random sleep')

    check = actions.add_parser('check', help='check ch default s3 credentials config status')
    check.set_defaults(func=_check_config)
    check.add_argument('-p', '--present', action='store_true', default=False,
                       help='whether config expected to present')

    return parser.parse_args()


def _get_token(endpoint):
    response = requests.get(f'http://{endpoint}/computeMetadata/v1/instance/service-accounts/default/token',
                            headers={'Metadata-Flavor': 'Google'})
    if response.status_code != 200:
        exit(1)
    data = json.loads(response.content)
    if data["token_type"] != "Bearer":
        exit(1)
    return data["access_token"]


def _add_node(document, root, name):
    node = document.createElement(name)
    root.appendChild(node)
    return node


def _update_config(args):
    if args.random_sleep:
        time.sleep(random.randint(0, 30))

    doc = minidom.Document()
    storage = _add_node(doc, _add_node(doc, _add_node(doc, doc, 'yandex'), 's3'), 'cloud_storage')
    _add_node(doc, storage, 'endpoint').appendChild(doc.createTextNode(args.endpoint))
    _add_node(doc, storage, 'header').appendChild(
        doc.createTextNode(f'X-YaCloud-SubjectToken: {_get_token(args.metadata_address)}'))
    with open('/etc/clickhouse-server/config.d/s3_credentials.xml', 'wb') as file:
        file.write(doc.toprettyxml(indent=4 * ' ', encoding='utf-8'))


def _delta_to_hours(delta: timedelta):
    return f'{(delta.total_seconds() / 3600):.2f}'


def _check_config(args):
    try:
        mtime = getmtime('/etc/clickhouse-server/config.d/s3_credentials.xml')
        if args.present:
            delta = timedelta(seconds=time.time() - mtime)
            if delta > timedelta(hours=12):
                print(f'2; S3 token expired {_delta_to_hours(delta - timedelta(hours=12))} hours ago')
            elif delta > timedelta(hours=4):
                print(f'2; S3 token expire in {_delta_to_hours(timedelta(hours=12) - delta)} hours')
            elif delta > timedelta(hours=2):
                print(f'1; S3 token expire in {_delta_to_hours(timedelta(hours=12) - delta)} hours')
            else:
                print('0; OK')
        else:
            print('2; S3 default config present, but shouldn\'t')
    except FileNotFoundError:
        if args.present:
            print('2; S3 default config not present')
        else:
            print('0; OK')


def main():
    args = _parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
