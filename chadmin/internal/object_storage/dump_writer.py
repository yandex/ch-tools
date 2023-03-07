import contextlib
import json
import sys

from contextlib import AbstractContextManager
from pathlib import Path
from typing import IO, Any, Literal, Self

from cloud.mdb.clickhouse.tools.chadmin.internal.object_storage.s3_object_metadata import CheckedMetaData

OBJECTS_WITH_METADATA_DUMP = 'objects_with_metadata.txt'
OBJECTS_WITHOUT_METADATA_DUMP = 'objects_without_metadata.txt'
OBJECTS_NOT_FOUND_DUMP = 'objects_not_found.txt'
UNPARSED_METADATA_DUMP = 'metadata_files_unparsed.txt'


class DumpWriter(AbstractContextManager):
    def write_object_with_metadata(self, obj: Any, metadata: CheckedMetaData) -> None:
        pass

    def write_object_without_metadata(self, obj: Any) -> None:
        pass

    def write_not_found_object(self, obj_key: str, metadata: CheckedMetaData) -> None:
        pass

    def write_unparsed_metadata(self, file_name: str) -> None:
        pass

    @property
    def objects_with_metadata_dump_path(self) -> Path | None:
        return None

    @property
    def objects_without_metadata_dump_path(self) -> Path | None:
        return None

    @property
    def not_found_objects_dump_path(self) -> Path | None:
        return None

    @property
    def unparsed_metadata_dump_path(self) -> Path | None:
        return None


class NullDumpWriter(DumpWriter):
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_args) -> Literal[False]:
        return False


class FileDumpWriter(DumpWriter, contextlib.ExitStack):
    def __init__(self, dump_dir: Path) -> None:
        super().__init__()

        self._dump_dir = dump_dir
        self._objects_with_metadata: IO | None = None
        self._objects_without_metadata: IO | None = None
        self._objects_not_found: IO | None = None
        self._metadata_files_unparsed: IO | None = None

    def __enter__(self) -> Self:
        super().__enter__()
        try:
            self._open_files()
        except Exception:
            if not self.__exit__(*sys.exc_info()):
                raise
        return self

    def write_object_with_metadata(self, obj: Any, metadata: CheckedMetaData) -> None:
        dict_line = {'object': {'key': obj.key, 'size': obj.size, 'last_modified': str(obj.last_modified)}, 'files': []}
        for file in metadata.files:
            dict_line['files'].append(str(file))

        self._objects_with_metadata.write(json.dumps(dict_line) + '\n')

    def write_object_without_metadata(self, obj: Any) -> None:
        dict_line = {'object': {'key': obj.key, 'size': obj.size, 'last_modified': str(obj.last_modified)}, 'files': []}

        self._objects_without_metadata.write(json.dumps(dict_line) + '\n')

    def write_not_found_object(self, obj_key: str, metadata: CheckedMetaData) -> None:
        dict_line = {'object': {'key': obj_key}, 'files': []}
        for file in metadata.files:
            dict_line['files'].append(str(file))

        self._objects_not_found.write(json.dumps(dict_line) + '\n')

    def write_unparsed_metadata(self, file_name: str) -> None:
        dict_line = {'file': file_name}

        self._metadata_files_unparsed.write(json.dumps(dict_line) + '\n')

    def _open_files(self) -> None:
        self._objects_with_metadata = self.enter_context(open(self.objects_with_metadata_dump_path, 'w'))
        self._objects_without_metadata = self.enter_context(open(self.objects_without_metadata_dump_path, 'w'))
        self._objects_not_found = self.enter_context(open(self.not_found_objects_dump_path, 'w'))
        self._metadata_files_unparsed = self.enter_context(open(self.unparsed_metadata_dump_path, 'w'))

    @property
    def objects_with_metadata_dump_path(self) -> Path | None:
        return self._dump_dir / OBJECTS_WITH_METADATA_DUMP

    @property
    def objects_without_metadata_dump_path(self) -> Path | None:
        return self._dump_dir / OBJECTS_WITHOUT_METADATA_DUMP

    @property
    def not_found_objects_dump_path(self) -> Path | None:
        return self._dump_dir / OBJECTS_NOT_FOUND_DUMP

    @property
    def unparsed_metadata_dump_path(self) -> Path | None:
        return self._dump_dir / UNPARSED_METADATA_DUMP
