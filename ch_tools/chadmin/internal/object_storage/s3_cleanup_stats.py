from collections import defaultdict
from datetime import datetime
from enum import Enum
from typing import TypedDict

from ch_tools.chadmin.internal.object_storage.obj_list_item import ObjListItem
from ch_tools.chadmin.internal.utils import DATETIME_FORMAT


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
