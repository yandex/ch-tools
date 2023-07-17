from typing import Any, Iterator, Tuple

import boto3  # type: ignore[import]
from botocore.client import Config  # type: ignore[import]

from ch_tools.chadmin.internal.object_storage.s3_disk_configuration import (
    S3DiskConfiguration,
)

ObjectSummary = Any
IGNORED_OBJECT_NAME_PREFIXES = ["operations", ".SCHEMA_VERSION"]


def s3_object_storage_iterator(
    disk: S3DiskConfiguration, object_name_prefix: str
) -> Iterator[Tuple[str, ObjectSummary]]:
    s3 = boto3.resource(
        "s3",
        endpoint_url=disk.endpoint_url,
        aws_access_key_id=disk.access_key_id,
        aws_secret_access_key=disk.secret_access_key,
        config=Config(s3={"addressing_style": "virtual"}),
    )
    bucket = s3.Bucket(disk.bucket_name)

    for obj in bucket.objects.filter(Prefix=disk.prefix + object_name_prefix):
        name: str = obj.key[len(disk.prefix) :]

        if _is_ignored(name):
            continue

        yield name, obj


def _is_ignored(name: str) -> bool:
    return any(name.startswith(p) for p in IGNORED_OBJECT_NAME_PREFIXES)
