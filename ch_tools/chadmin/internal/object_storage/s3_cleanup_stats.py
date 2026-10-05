from collections import defaultdict
from datetime import datetime
from enum import Enum
from typing import TypedDict

from ch_tools.chadmin.internal.object_storage.obj_list_item import ObjListItem
from ch_tools.chadmin.internal.utils import DATETIME_FORMAT


class StatisticsPeriod(str, Enum):
    """
    How to partition stats of deleted orphaned objects
    """

    DAY = "day"
    MONTH = "month"
    ALL = "all"


class StatDict(TypedDict):
    total_count: int
    total_size: int
    deleted_count: int
    deleted_size: int


class ResultStat(defaultdict):
    def _default_factory(self) -> StatDict:
        return {
            "total_count": 0,
            "total_size": 0,
            "deleted_count": 0,
            "deleted_size": 0,
        }

    def __init__(
        self, stat_partitioning: StatisticsPeriod = StatisticsPeriod.ALL
    ) -> None:
        super().__init__(self._default_factory)
        self._stat_partitioning = stat_partitioning

    @property
    def total(self) -> StatDict:
        return self["Total"]

    def update_total(self, count: int, total_size: int) -> None:
        self.total["total_count"] = count
        self.total["total_size"] = total_size

    def update_by_item(self, item: ObjListItem) -> None:
        self.total["deleted_count"] += 1
        self.total["deleted_size"] += item.size

        if self._stat_partitioning == StatisticsPeriod.ALL:
            return

        key = self._get_stat_key(item.last_modified)
        self[key]["deleted_count"] += 1
        self[key]["deleted_size"] += item.size

    def _get_stat_key(self, timestamp: datetime) -> str:
        if self._stat_partitioning == StatisticsPeriod.ALL:
            return "Total"
        time_str = timestamp.strftime(DATETIME_FORMAT)
        if self._stat_partitioning == StatisticsPeriod.MONTH:
            ymd = time_str.split("-")
            return "-".join(ymd[:2])
        if self._stat_partitioning == StatisticsPeriod.DAY:
            ymd_hms = time_str.split(" ")
            return ymd_hms[0]
