from ch_tools.chadmin.internal.object_storage.obj_list_item import ObjListItem
from ch_tools.chadmin.internal.object_storage.s3_cleanup_stats import (
    ResultStat,
    StatisticsPeriod,
)


def test_default_keys() -> None:
    stat = ResultStat()

    assert stat.total == {"total_size": 0, "deleted": 0}
    assert len(stat.items()) == 1


def test_update() -> None:
    stat = ResultStat()
    item = ObjListItem.from_tab_separated("2025-11-05 10:15:20\tsome/path/on/s3\t4")
    stat.update_by_item(item)

    assert len(stat.items()) == 1
    assert stat.total == {"total_size": 4, "deleted": 1}

    stat.update_by_item(item)
    assert stat.total == {"total_size": 8, "deleted": 2}


def test_partitioning_month() -> None:
    stat = ResultStat(StatisticsPeriod.MONTH)
    item1 = ObjListItem.from_tab_separated("2025-11-05 10:15:20\tsome/path/on/s3\t4")
    item2 = ObjListItem.from_tab_separated("2025-10-05 10:15:20\tsome/path/on/s3\t2")

    stat.update_by_item(item1)
    stat.update_by_item(item2)

    assert len(stat.items()) == 3
    assert stat.total == {"total_size": 6, "deleted": 2}
    assert stat["2025-11"] == {"total_size": 4, "deleted": 1}
    assert stat["2025-10"] == {"total_size": 2, "deleted": 1}


def test_partitioning_day() -> None:
    stat = ResultStat(StatisticsPeriod.DAY)
    item1 = ObjListItem.from_tab_separated("2025-11-05 10:15:20\tsome/path/on/s3\t4")
    item2 = ObjListItem.from_tab_separated("2025-11-06 10:15:20\tsome/path/on/s3\t2")

    stat.update_by_item(item1)
    stat.update_by_item(item2)

    assert len(stat.items()) == 3
    assert stat.total == {"total_size": 6, "deleted": 2}
    assert stat["2025-11-05"] == {"total_size": 4, "deleted": 1}
    assert stat["2025-11-06"] == {"total_size": 2, "deleted": 1}
