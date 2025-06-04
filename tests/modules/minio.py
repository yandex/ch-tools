"""
Interface to Minio S3 server.
"""

import json
import os

from docker.models.containers import Container
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from .docker import copy_container_dir, get_container
from .typing import ContextT


class MinioException(Exception):
    """
    Minion exception.
    """

    def __init__(self, response: dict) -> None:
        super().__init__(self._fmt_message(response))
        self.response = response

    @staticmethod
    def _fmt_message(response: dict) -> str:
        try:
            error = response["error"]
            message = f'{error["message"]} Cause: {error["cause"]["message"]}'

            code = error["cause"]["error"].get("Code")
            if code:
                message = f"{message} [{code}]"

            return message

        except Exception:
            return f"Failed with response: {response}"


class BucketAlreadyOwnedByYou(MinioException):
    """
    BucketAlreadyOwnedByYou Minion exception.
    """

    pass


def initialize(context: ContextT) -> None:
    """
    Initialize Minio server.
    """
    _configure_s3_credentials(context)
    _create_s3_bucket(context)


def export_s3_data(context: ContextT, path: str) -> None:
    """
    Export S3 data to the specified directory.
    """
    local_dir = os.path.join(path, "minio")
    copy_container_dir(_container(context), "/export", local_dir)


@retry(
    retry=retry_if_exception_type(MinioException),
    wait=wait_fixed(1),
    stop=stop_after_attempt(10),
)
def _configure_s3_credentials(context: ContextT) -> None:
    """
    Configure S3 credentials in mc (Minio client).
    """
    access_key = context.conf["s3"]["access_key_id"]
    secret_key = context.conf["s3"]["access_secret_key"]
    _mc_execute(
        context,
        f"config host add local http://localhost:9000 {access_key} {secret_key}",
    )


def _create_s3_bucket(context: ContextT) -> None:
    """
    Create S3 bucket specified in the config.
    """
    bucket = context.conf["s3"]["bucket"]
    try:
        _mc_execute(context, f"mb local/{bucket}")
    except BucketAlreadyOwnedByYou:
        pass


def _container(context: ContextT) -> Container:
    return get_container(context, "minio01")


def _mc_execute(context: ContextT, command: str) -> dict:
    """
    Execute mc (Minio client) command.
    """
    output = _container(context).exec_run(f"mc --json {command}").output.decode()
    response = json.loads(output)
    if response["status"] == "success":
        return response

    error_code = response["error"]["cause"]["error"].get("Code")
    exception_types = {
        "BucketAlreadyOwnedByYou": BucketAlreadyOwnedByYou,
    }
    raise exception_types.get(error_code, MinioException)(response)
