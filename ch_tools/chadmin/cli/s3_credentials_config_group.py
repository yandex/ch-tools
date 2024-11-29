import json
import random
import sys
import time
from xml.dom import minidom

import requests
from click import group, option, pass_context

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.system import match_ch_version


@group("s3-credentials-config", cls=Chadmin)
@option(
    "-m",
    "--metadata-address",
    "metadata_address",
    default="169.254.169.254",
    help="Compute metadata api address.",
)
@pass_context
def s3_credentials_config_group(ctx, metadata_address):
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


@s3_credentials_config_group.command(
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
    endpoint_header = (
        "access_header" if match_ch_version(ctx, min_version="24.11") else "header"
    )
    _add_node(doc, storage, "endpoint").appendChild(doc.createTextNode(endpoint))
    _add_node(doc, storage, endpoint_header).appendChild(
        doc.createTextNode(
            f"X-YaCloud-SubjectToken: {_get_token(ctx.obj['s3_cred_metadata_addr'])}"
        )
    )
    with open("/etc/clickhouse-server/config.d/s3_credentials.xml", "wb") as file:
        file.write(doc.toprettyxml(indent=4 * " ", encoding="utf-8"))
