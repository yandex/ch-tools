from typing import List, TypedDict


class TableInfo(TypedDict):
    """Table information."""

    database: str
    name: str
    uuid: str
    engine: str
    create_table_query: str
    metadata_path: str
    metadata_modification_time: str
    data_paths: List[str]
    disk_size: int
    partitions: int
    parts: int
    rows: int
