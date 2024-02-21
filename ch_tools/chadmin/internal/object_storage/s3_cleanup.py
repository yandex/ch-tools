from typing import Any, Iterator, List, Tuple

import boto3
from botocore.client import Config

from ch_tools.chadmin.internal.object_storage import ObjListItem
from ch_tools.chadmin.internal.utils import chunked
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration

BULK_DELETE_CHUNK_SIZE = 1000


def cleanup_s3_object_storage(
    disk: S3DiskConfiguration, keys: Iterator[ObjListItem], dry_run: bool = False
) -> Tuple[int, int]:
    s3 = boto3.resource(
        "s3",
        endpoint_url=disk.endpoint_url,
        aws_access_key_id=disk.access_key_id,
        aws_secret_access_key=disk.secret_access_key,
        config=Config(s3={"addressing_style": "auto"}),
    )
    bucket = s3.Bucket(disk.bucket_name)
    deleted = 0
    total_size = 0

    for chunk in chunked(keys, BULK_DELETE_CHUNK_SIZE):
        if not dry_run:
            _bulk_delete(bucket, chunk)
        deleted += len(chunk)
        total_size += sum(item.size for item in chunk)

    return deleted, total_size


def _bulk_delete(bucket: Any, items: List[ObjListItem]) -> None:
    objects = [{"Key": item.path} for item in items]
    bucket.delete_objects(Delete={"Objects": objects, "Quiet": False})
