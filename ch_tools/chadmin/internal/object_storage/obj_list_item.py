from dataclasses import dataclass


@dataclass
class ObjListItem:
    """
    Item of object storage listing.
    """

    path: str
    size: int

    @classmethod
    def from_tab_separated(cls, value: str) -> "ObjListItem":
        path, size = value.split("\t")
        return cls(path, int(size))
