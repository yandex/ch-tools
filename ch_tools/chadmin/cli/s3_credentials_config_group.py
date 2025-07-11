import json
import random
import sys
import time
from typing import Any
from xml.dom import minidom

import requests
from click import Context, group, option, pass_context

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.system import match_ch_version
from ch_tools.common.clickhouse.config.path import CLICKHOUSE_S3_CREDENTIALS_CONFIG_PATH


@group("s3-credentials-config", cls=Chadmin)
def s3_credentials_config_group() -> None:
    """
    Commands to manage S3 credentials config.
    """
    pass


@s3_credentials_config_group.command("update")
@option(
    "-e",
    "--endpoint",
    "s3_endpoint",
    type=str,
    required=True,
    help="S3 endpoint.",
)
@option(
    "-s",
    "--random-sleep",
    "random_sleep",
    is_flag=True,
    default=False,
    help="Perform random sleep before updating S3 credentials config.",
)
@pass_context
def update_s3_credentials(ctx: Context, s3_endpoint: str, random_sleep: bool) -> None:
    """Update S3 credentials config."""
    if random_sleep:
        time.sleep(random.randint(0, 30))

    doc = minidom.Document()
    storage = _add_xml_node(
        doc,
        _add_xml_node(doc, _add_xml_node(doc, doc, "clickhouse"), "s3"),
        "cloud_storage",
    )
    endpoint_header = (
        "access_header" if match_ch_version(ctx, min_version="24.11") else "header"
    )
    _add_xml_node(doc, storage, "endpoint").appendChild(doc.createTextNode(s3_endpoint))
    _add_xml_node(doc, storage, endpoint_header).appendChild(
        doc.createTextNode(f"X-YaCloud-SubjectToken: {_get_token(ctx)}")
    )

    with open(CLICKHOUSE_S3_CREDENTIALS_CONFIG_PATH, "wb") as file:
        file.write(doc.toprettyxml(indent=4 * " ", encoding="utf-8"))


def _add_xml_node(document: minidom.Document, root: Any, name: str) -> minidom.Element:
    node = document.createElement(name)
    root.appendChild(node)
    return node


def _get_token(ctx: Context) -> str:
    response = _request_token(ctx)
    if response.status_code != 200:
        sys.exit(1)
    data = json.loads(response.content)
    if data["token_type"] != "Bearer":
        sys.exit(1)
    return data["access_token"]


def _request_token(ctx: Context) -> requests.Response:
    endpoint = ctx.obj["config"]["cloud"]["metadata_service_endpoint"]
    return requests.get(
        f"{endpoint}/computeMetadata/v1/instance/service-accounts/default/token",
        headers={"Metadata-Flavor": "Google"},
        timeout=60,
    )
