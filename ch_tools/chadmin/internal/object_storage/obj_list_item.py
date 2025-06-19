import os
import uuid
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


class ObjListIterator:

    def __init__(self, ch_client, query, timeout, settings, use_stream, tmp_dir_path):
        os.makedirs(tmp_dir_path, exist_ok=True)

        self.file_path = os.path.join(tmp_dir_path, f"object_list_{str(uuid.uuid4())}")
        with open(self.file_path, "w", encoding="utf-8") as file:
            with ch_client.query(
                query,
                timeout=timeout,
                settings=settings,
                stream=use_stream,
                format_="TabSeparated",
            ) as resp:
                for line in resp.iter_lines():
                    file.write(line.decode() + "\n")

    def __iter__(self):
        with open(self.file_path, "r", encoding="utf-8") as file:
            for line in file:
                yield ObjListItem.from_tab_separated(line)

    def __del__(self):
        os.remove(self.file_path)
