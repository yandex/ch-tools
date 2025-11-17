from typing import Any, Iterator, List

import boto3
from botocore.client import Config

from ch_tools.chadmin.internal.object_storage.s3_cleanup_stats import ResultStat
from ch_tools.chadmin.internal.utils import chunked
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration

from .obj_list_item import ObjListItem

BULK_DELETE_CHUNK_SIZE = 1000


def cleanup_s3_object_storage(
    disk: S3DiskConfiguration,
    keys: Iterator[ObjListItem],
    stat: ResultStat,
    dry_run: bool = False,
) -> None:
    s3 = boto3.resource(
        "s3",
        endpoint_url=disk.endpoint_url,
        aws_access_key_id=disk.access_key_id,
        aws_secret_access_key=disk.secret_access_key,
        config=Config(
            s3={
                "addressing_style": "auto",
            },
        ),
    )
    bucket = s3.Bucket(disk.bucket_name)

    for chunk in chunked(keys, BULK_DELETE_CHUNK_SIZE):
        if not dry_run:
            _bulk_delete(bucket, chunk)
        for item in chunk:
            stat.update_by_item(item)


def _bulk_delete(bucket: Any, items: List[ObjListItem]) -> None:
    objects = [{"Key": item.path} for item in items]
    bucket.delete_objects(Delete={"Objects": objects, "Quiet": False})
