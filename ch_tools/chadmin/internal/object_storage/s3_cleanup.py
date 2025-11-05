from collections import defaultdict
from datetime import datetime
from enum import Enum
from typing import Any, Iterator, List, TypedDict

import boto3
from botocore.client import Config

from ch_tools.chadmin.internal.utils import DATETIME_FORMAT, chunked
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration

from .obj_list_item import ObjListItem

BULK_DELETE_CHUNK_SIZE = 1000


class StatisticsPartitioning(str, Enum):
    """
    How to partition stats of deleted orphaned objects
    """

    DAY = "day"
    MONTH = "month"
    ALL = "all"


class StatDict(TypedDict):
    deleted: int
    total_size: int


class ResultStat(defaultdict):
    def _default_factory(self) -> StatDict:
        return {"deleted": 0, "total_size": 0}

    def __init__(
        self, stat_partitioning: StatisticsPartitioning = StatisticsPartitioning.ALL
    ) -> None:
        super().__init__(self._default_factory)
        self._stat_partitioning = stat_partitioning

    @property
    def total(self) -> StatDict:
        return self["Total"]

    def update_by_item(self, item: ObjListItem) -> None:
        self.total["deleted"] += 1
        self.total["total_size"] += item.size

        if self._stat_partitioning == StatisticsPartitioning.ALL:
            return

        key = self._get_stat_key(item.last_modified)
        self[key]["deleted"] += 1
        self[key]["total_size"] += item.size

    def _get_stat_key(self, timestamp: datetime) -> str:
        if self._stat_partitioning == StatisticsPartitioning.ALL:
            return "Total"
        time_str = timestamp.strftime(DATETIME_FORMAT)
        if self._stat_partitioning == StatisticsPartitioning.MONTH:
            ymd = time_str.split("-")
            return "-".join(ymd[:2])
        if self._stat_partitioning == StatisticsPartitioning.DAY:
            ymd_hms = time_str.split(" ")
            return ymd_hms[0]


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
