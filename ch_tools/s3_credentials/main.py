#!/usr/bin/env python3
"""
Manage default ClickHouse s3 credentials.
"""

import argparse
import json
import os.path
import random
import sys
import time
from datetime import timedelta
from xml.dom import minidom

import requests

from ch_tools.common.clickhouse.config.path import (
    CLICKHOUSE_RESETUP_CONFIG_PATH,
    CLICKHOUSE_S3_CREDENTIALS_CONFIG_PATH,
)


def _parse_args():
    parser = argparse.ArgumentParser()
    actions = parser.add_subparsers()

    parser.add_argument(
        "-m",
        "--metadata-address",
        type=str,
        default="169.254.169.254",
        help="compute metadata api address",
    )

    update = actions.add_parser(
        "update", help="update ch default s3 credentials config"
    )
    update.set_defaults(func=_update_config)
    update.add_argument("-e", "--endpoint", type=str, help="S3 endpoint")
    update.add_argument(
        "-s",
        "--random-sleep",
        action="store_true",
        default=False,
        help="whether need a random sleep",
    )

    check = actions.add_parser(
        "check", help="check ch default s3 credentials config status"
    )
    check.set_defaults(func=_check_config)
    check.add_argument(
        "-p",
        "--present",
        action="store_true",
        default=False,
        help="whether config expected to present",
    )

    return parser.parse_args()


def _request_token(endpoint):
    # pylint: disable=missing-timeout
    return requests.get(
        f"http://{endpoint}/computeMetadata/v1/instance/service-accounts/default/token",
        headers={"Metadata-Flavor": "Google"},
    )


def _get_token(endpoint):
    response = _request_token(endpoint)
    if response.status_code != 200:
        sys.exit(1)
    data = json.loads(response.content)
    if data["token_type"] != "Bearer":
        sys.exit(1)
    return data["access_token"]


def _add_node(document, root, name):
    node = document.createElement(name)
    root.appendChild(node)
    return node


def _update_config(args):
    if args.random_sleep:
        time.sleep(random.randint(0, 30))

    doc = minidom.Document()
    storage = _add_node(
        doc, _add_node(doc, _add_node(doc, doc, "yandex"), "s3"), "cloud_storage"
    )
    _add_node(doc, storage, "endpoint").appendChild(doc.createTextNode(args.endpoint))
    _add_node(doc, storage, "header").appendChild(
        doc.createTextNode(
            f"X-YaCloud-SubjectToken: {_get_token(args.metadata_address)}"
        )
    )
    with open("/etc/clickhouse-server/config.d/s3_credentials.xml", "wb") as file:
        file.write(doc.toprettyxml(indent=4 * " ", encoding="utf-8"))


def _delta_to_hours(delta: timedelta) -> str:
    return f"{(delta.total_seconds() / 3600):.2f}"


def _check_config(args):
    # pylint: disable=missing-timeout
    if not args.present:
        if os.path.exists(CLICKHOUSE_S3_CREDENTIALS_CONFIG_PATH):
            result(2, "S3 default config present, but shouldn't")
        else:
            result(0, "OK")

    if os.path.isfile(CLICKHOUSE_RESETUP_CONFIG_PATH):
        result(0, "Skipped as resetup is in progress")

    if os.path.exists(CLICKHOUSE_S3_CREDENTIALS_CONFIG_PATH):
        delta = timedelta(
            seconds=time.time()
            - os.path.getmtime(CLICKHOUSE_S3_CREDENTIALS_CONFIG_PATH)
        )
        if delta < timedelta(hours=2):
            result(0, "OK")
        elif delta < timedelta(hours=4):
            result(
                1,
                f"S3 token expire in {_delta_to_hours(timedelta(hours=12) - delta)} hours",
            )

        if delta < timedelta(hours=12):
            msg = f"S3 token expire in {_delta_to_hours(timedelta(hours=12) - delta)} hours"
        else:
            msg = f"S3 token expired {_delta_to_hours(delta - timedelta(hours=12))} hours ago"
    else:
        msg = "S3 default config not present"

    code = _request_token(args.metadata_address).status_code
    if code == 404:
        if "default" in requests.get(
            f"http://{args.metadata_address}/computeMetadata/v1/instance/?recursive=true",
            headers={"Metadata-Flavor": "Google"},
        ).json().get("serviceAccounts", {}):
            result(1, "service account deleted")
        else:
            result(2, "service account not linked")

    result(2, msg + f", iam code {code}")


def result(code, msg):
    print(f"{code};{msg}")
    sys.exit(0)


def main():
    """
    Program entry point.
    """
    args = _parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
