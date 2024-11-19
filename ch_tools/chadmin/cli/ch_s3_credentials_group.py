import json
import os.path
import random
import sys
import time
from datetime import timedelta
from xml.dom import minidom

import requests
from click import group, option, pass_context

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.system import match_ch_version
from ch_tools.common.clickhouse.config.path import (
    CLICKHOUSE_RESETUP_CONFIG_PATH,
    CLICKHOUSE_S3_CREDENTIALS_CONFIG_PATH,
)


@group("ch-s3-credentials", cls=Chadmin)
@option(
    "-m",
    "--metadata-address",
    "metadata_address",
    default="169.254.169.254",
    help="Compute metadata api address.",
)
@pass_context
def ch_s3_credentials_group(ctx, metadata_address):
    """
    Manage default ClickHouse s3 credentials.
    """
    ctx.obj["s3_cred_metadata_addr"] = metadata_address


def _add_node(document, root, name):
    node = document.createElement(name)
    root.appendChild(node)
    return node


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


@ch_s3_credentials_group.command(
    "update", help="update ch default s3 credentials config"
)
@option("-e", "--endpoint", "endpoint", type=str, help="S3 endpoint")
@option(
    "-s",
    "--random-sleep",
    "random_sleep",
    default=False,
    help="whether need a random sleep",
)
@pass_context
def update_s3_credentials(ctx, endpoint, random_sleep):
    """Update s3 creds."""
    if random_sleep:
        time.sleep(random.randint(0, 30))

    doc = minidom.Document()
    storage = _add_node(
        doc, _add_node(doc, _add_node(doc, doc, "clickhouse"), "s3"), "cloud_storage"
    )
    endpoint_header = "access_header" if match_ch_version(ctx, min_version="24.11") else "header"
    _add_node(doc, storage, "endpoint").appendChild(doc.createTextNode(endpoint))
    _add_node(doc, storage, endpoint_header).appendChild(
        doc.createTextNode(
            f"X-YaCloud-SubjectToken: {_get_token(ctx.obj['s3_cred_metadata_addr'])}"
        )
    )
    with open("/etc/clickhouse-server/config.d/s3_credentials.xml", "wb") as file:
        file.write(doc.toprettyxml(indent=4 * " ", encoding="utf-8"))


def result(code, msg):
    print(f"{code};{msg}")
    sys.exit(0)


def _delta_to_hours(delta: timedelta) -> str:
    return f"{(delta.total_seconds() / 3600):.2f}"


@ch_s3_credentials_group.command(
    "check", help="check ch default s3 credentials config status"
)
@option(
    "-p",
    "--present",
    default=False,
    is_flag=True,
    help="whether config expected to present",
)
@pass_context
def check_s3_credentials(ctx, present):
    # pylint: disable=missing-timeout
    if not present:
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

    code = _request_token(ctx.obj["s3_cred_metadata_addr"]).status_code
    if code == 404:
        if "default" in requests.get(
            f"http://{ctx.obj['s3_cred_metadata_addr']}/computeMetadata/v1/instance/?recursive=true",
            headers={"Metadata-Flavor": "Google"},
        ).json().get("serviceAccounts", {}):
            result(1, "service account deleted")
        else:
            result(2, "service account not linked")

    result(2, msg + f", iam code {code}")
