import json
from dataclasses import dataclass
from datetime import datetime

from ch_tools.chadmin.internal.utils import DATETIME_FORMAT


@dataclass
class ObjListItem:
    """
    Item of object storage listing.
    """

    last_modified: datetime
    path: str
    size: int

    @classmethod
    def from_tab_separated(cls, value: str) -> "ObjListItem":
        time_str, path, size = value.split("\t")
        last_modified = datetime.strptime(time_str, DATETIME_FORMAT)
        return cls(last_modified, path, int(size))

    @classmethod
    def from_json(cls, value: str) -> "ObjListItem":
        parsed_json = json.loads(value)
        last_modified = datetime.strptime(parsed_json["last_modified"], DATETIME_FORMAT)
        return cls(last_modified, parsed_json["obj_path"], parsed_json["obj_size"])
