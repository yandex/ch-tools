from enum import Enum
from typing import Any, Iterator, List

import boto3
from botocore.client import Config

from ch_tools.chadmin.internal.utils import DATETIME_FORMAT, chunked
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration

from .obj_list_item import ObjListItem

BULK_DELETE_CHUNK_SIZE = 1000


class ResultStat:
    def __getitem__(self, key: str) -> dict[str, int]:
        if key not in self._dict:
            self._init_key(key)
        return self._dict[key]

    def __setitem__(self, key: str, value: dict[str, int]) -> None:
        self._dict[key] = value

    def __init__(self) -> None:
        self._dict: dict[str, dict[str, int]] = {}
        self._init_key("Total")

    def _init_key(self, key: str) -> None:
        self[key] = {"deleted": 0, "total_size": 0}

    def items(self) -> Any:
        return self._dict.items()


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
    stat: ResultStat,
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
    stat: ResultStat, stat_partitioning: StatisticsPartitioning, item: ObjListItem
) -> None:
    stat["Total"]["deleted"] += 1
    stat["Total"]["total_size"] += item.size

    key = _get_stat_key_by_partitioning(
        stat_partitioning, item.last_modified.strftime(DATETIME_FORMAT)
    )
    if key != "Total":
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
