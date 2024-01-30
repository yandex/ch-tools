from typing import Any, Iterator, List

import boto3
from botocore.client import Config  # type: ignore[import]

from ch_tools.chadmin.internal.utils import chunked
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration

BULK_DELETE_CHUNK_SIZE = 1000


def cleanup_s3_object_storage(
    disk: S3DiskConfiguration, keys: Iterator[str], dry_run: bool = False
) -> int:
    s3 = boto3.resource(
        "s3",
        endpoint_url=disk.endpoint_url,
        aws_access_key_id=disk.access_key_id,
        aws_secret_access_key=disk.secret_access_key,
        config=Config(s3={"addressing_style": "auto"}),
    )
    bucket = s3.Bucket(disk.bucket_name)
    deleted = 0

    for chunk in chunked(keys, BULK_DELETE_CHUNK_SIZE):
        if not dry_run:
            _bulk_delete(bucket, chunk)
        deleted += len(chunk)

    return deleted


def _bulk_delete(bucket: Any, keys: List[str]) -> None:
    objects = [{"Key": key} for key in keys]
    bucket.delete_objects(Delete={"Objects": objects, "Quiet": False})
