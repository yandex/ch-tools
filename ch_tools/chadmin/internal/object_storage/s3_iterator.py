from typing import Any, Iterator

import boto3  # type: ignore[import]
from botocore.client import Config

from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration

ObjectSummary = Any
IGNORED_OBJECT_NAME_PREFIXES = ["operations", ".SCHEMA_VERSION"]


def s3_object_storage_iterator(
    disk: S3DiskConfiguration,
    *,
    object_name_prefix: str = "",
    skip_ignoring: bool = False,
) -> Iterator[ObjectSummary]:
    s3 = boto3.resource(
        "s3",
        endpoint_url=disk.endpoint_url,
        aws_access_key_id=disk.access_key_id,
        aws_secret_access_key=disk.secret_access_key,
        config=Config(s3={"addressing_style": "auto"}),
    )
    bucket = s3.Bucket(disk.bucket_name)

    for obj in bucket.objects.filter(Prefix=object_name_prefix):
        if not skip_ignoring and _is_ignored(obj.key):
            continue
        yield obj


def _is_ignored(name: str) -> bool:
    return any(p in name for p in IGNORED_OBJECT_NAME_PREFIXES)
