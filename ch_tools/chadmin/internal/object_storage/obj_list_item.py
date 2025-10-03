from dataclasses import dataclass
from datetime import datetime

from ch_tools.chadmin.internal.utils import DATETIME_FORMAT


@dataclass
class ObjListItem:
    """
    Item of object storage listing.
    """

    time: datetime
    path: str
    size: int

    @classmethod
    def from_tab_separated(cls, value: str) -> "ObjListItem":
        time_str, path, size = value.split("\t")
        time = datetime.strptime(time_str, DATETIME_FORMAT)
        return cls(time, path, int(size))
