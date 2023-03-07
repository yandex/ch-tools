import json

from pathlib import Path
from typing import Any, Iterator

from cloud.mdb.clickhouse.tools.chadmin.internal.utils import chunked

BULK_DELETE_CHUNK_SIZE = 1000


class S3Cleaner:
    """
    Remove objects from S3 bucket.

    S3 object keys for deletion are read from dump file created by S3 analyzer.
    """

    def __init__(self, bucket: Any) -> None:
        self._bucket = bucket

    def clean_dumped_objects(self, dump_path: Path) -> int:
        deleted = 0

        keys_itr = self._read_keys_from_dump(dump_path)
        for chunk in chunked(keys_itr, BULK_DELETE_CHUNK_SIZE):
            self._bulk_delete(chunk)
            deleted += len(chunk)

        return deleted

    def _read_keys_from_dump(self, dump_path: Path) -> Iterator[str]:
        with dump_path.open() as dump_file:
            for row in dump_file:
                key = self._parse_key_from_dump_line(row)
                if not key:
                    continue
                yield key

    def _bulk_delete(self, keys: list[str]) -> None:
        objects = [{'Key': key} for key in keys]
        self._bucket.delete_objects(Delete={'Objects': objects, 'Quiet': False})

    @staticmethod
    def _parse_key_from_dump_line(line: str) -> str | None:
        try:
            return json.loads(line)['object']['key']
        except Exception:
            return None
