import os
import time
from datetime import timedelta
from typing import Any

import requests
from click import Context, pass_context
from cloup import command, option

from ch_tools.common import logging
from ch_tools.common.clickhouse.config.path import (
    CLICKHOUSE_RESETUP_CONFIG_PATH,
    CLICKHOUSE_S3_CREDENTIALS_CONFIG_PATH,
)
from ch_tools.common.result import CRIT, OK, WARNING, Result


@command("s3-credentials-config")
@option(
    "-p",
    "--present/--missing",
    default=False,
    is_flag=True,
    help="Whether S3 credentials config should be present or not.",
)
@pass_context
def s3_credentials_configs_command(ctx: Context, present: bool) -> Result:
    """
    Check S3 credentials config.
    """
    # pylint: disable=too-many-return-statements
    try:
        if not present:
            if not os.path.exists(CLICKHOUSE_S3_CREDENTIALS_CONFIG_PATH):
                return Result(OK)
            return Result(CRIT, "S3 credentials config exists, but shouldn't")

        if os.path.isfile(CLICKHOUSE_RESETUP_CONFIG_PATH):
            return Result(OK, "Skipped as resetup is in progress")

        if os.path.exists(CLICKHOUSE_S3_CREDENTIALS_CONFIG_PATH):
            delta = timedelta(
                seconds=time.time()
                - os.path.getmtime(CLICKHOUSE_S3_CREDENTIALS_CONFIG_PATH)
            )
            if delta < timedelta(hours=2):
                return Result(OK)
            if delta < timedelta(hours=4):
                return Result(
                    WARNING,
                    f"S3 token expire in {_delta_to_hours(timedelta(hours=12) - delta)} hours",
                )

            if delta < timedelta(hours=12):
                msg = f"S3 token expire in {_delta_to_hours(timedelta(hours=12) - delta)} hours"
            else:
                msg = f"S3 token expired {_delta_to_hours(delta - timedelta(hours=12))} hours ago"
        else:
            msg = "S3 credentials config is missing"

        endpoint = ctx.obj["config"]["cloud"]["metadata_service_endpoint"]
        code = _request_token(endpoint).status_code
        if code == 404:
            if "default" in requests.get(
                f"{endpoint}/computeMetadata/v1/instance/?recursive=true",
                headers={"Metadata-Flavor": "Google"},
                timeout=60,
            ).json().get("serviceAccounts", {}):
                return Result(WARNING, "service account deleted")

            return Result(CRIT, "service account not linked")

        return Result(CRIT, f"{msg}, IAM code {code}")

    except Exception:
        logging.exception("Failed to check S3 credentials config")
        return Result(CRIT, "Internal error")


def _request_token(metadata_service_endpoint: str) -> Any:
    return requests.get(
        f"{metadata_service_endpoint}/computeMetadata/v1/instance/service-accounts/default/token",
        headers={"Metadata-Flavor": "Google"},
        timeout=60,
    )


def _delta_to_hours(delta: timedelta) -> str:
    return f"{(delta.total_seconds() / 3600):.2f}"
