from dataclasses import dataclass
from datetime import datetime

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


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
