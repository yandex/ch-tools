from enum import Enum
from typing import Any, Iterator, List

import boto3
from botocore.client import Config

from ch_tools.chadmin.internal.utils import DATETIME_FORMAT, chunked
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration

from .obj_list_item import ObjListItem

BULK_DELETE_CHUNK_SIZE = 1000
ResultStatDict = dict[str, dict[str, int]]


class StatisticsPartitioning(str, Enum):
    """
    How to partition stats of deleted orphaned objects
    """

    DAY = "day"
    MONTH = "month"
    ALL = "all"


def cleanup_s3_object_storage(
    disk: S3DiskConfiguration,
    keys: Iterator[ObjListItem],
    stat: ResultStatDict,
    stat_partitioning: StatisticsPartitioning,
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
            _update_stat(stat, stat_partitioning, item)


def _bulk_delete(bucket: Any, items: List[ObjListItem]) -> None:
    objects = [{"Key": item.path} for item in items]
    bucket.delete_objects(Delete={"Objects": objects, "Quiet": False})


def _update_stat(
    stat: ResultStatDict, stat_partitioning: StatisticsPartitioning, item: ObjListItem
) -> None:
    if "Total" not in stat:
        _init_key_in_stat(stat, "Total")
    stat["Total"]["deleted"] += 1
    stat["Total"]["total_size"] += item.size

    key = _get_stat_key_by_partitioning(
        stat_partitioning, item.last_modified.strftime(DATETIME_FORMAT)
    )
    if key != "Total":
        _init_key_in_stat(stat, key)
        stat[key]["deleted"] += 1
        stat[key]["total_size"] += item.size


def _get_stat_key_by_partitioning(
    stat_partitioning: StatisticsPartitioning, full_key: str
) -> str:
    if stat_partitioning == StatisticsPartitioning.ALL:
        return "Total"
    if stat_partitioning == StatisticsPartitioning.MONTH:
        ymd = full_key.split("-")
        return "-".join(ymd[:2])
    if stat_partitioning == StatisticsPartitioning.DAY:
        ymd_hms = full_key.split(" ")
        return ymd_hms[0]


def _init_key_in_stat(stat: ResultStatDict, key: str) -> None:
    if key not in stat:
        stat[key] = {"deleted": 0, "total_size": 0}
