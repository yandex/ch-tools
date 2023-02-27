import contextlib
import logging

from collections import defaultdict
from dataclasses import dataclass, field
from itertools import chain
from pathlib import Path
from typing import IO, Any, Iterable, Iterator

from cloud.mdb.clickhouse.tools.chadmin.internal.object_storage.s3_analyzer_dump_paths import S3AnalyzerDumpPaths
from cloud.mdb.clickhouse.tools.chadmin.internal.object_storage.s3_object_metadata import S3ObjectLocalMetaData
from cloud.mdb.clickhouse.tools.chadmin.internal.utils import open_if_not_none

# The name of the data S3 object looks like
# r0000000000000000000000000000000000000000000000000000000000000001-file-btkncsrzgigakclscqdgcomafjrjaaya
DATA_OBJ_MAME_PREFIX = 'r0'


@dataclass
class CheckedMetaData:
    files: dict[Path, S3ObjectLocalMetaData] = field(default_factory=dict)
    checked: bool = False


MetaDataByObjectKey = dict[str, CheckedMetaData]


@dataclass
class S3AnalyzerStats:
    existing_objects_with_metadata_count: int = 0
    existing_objects_with_metadata_total_size: int = 0
    existing_objects_without_metadata_count: int = 0
    existing_objects_without_metadata_total_size: int = 0
    non_existing_objects_with_metadata_count: int = 0
    metadata_files_unparsed: int = 0


class S3Analyzer:
    def __init__(self, bucket: Any, prefix: str | None = None, dump_paths: S3AnalyzerDumpPaths | None = None) -> None:
        self._bucket = bucket
        self._dump_paths = dump_paths

        self._base_prefix = prefix if prefix else ''
        # When listing objects we are only interested in data objects. A bucket can also contain other objects,
        # such as operations with names like {base_prefix}operations/...
        self._list_prefix = self._base_prefix + DATA_OBJ_MAME_PREFIX

        self._stats = S3AnalyzerStats()

    @contextlib.contextmanager
    def open_dump_files_if_needed(self) -> Iterator[list[IO | None]]:
        with open_if_not_none(
            self._dump_paths.objects_with_metadata if self._dump_paths else None,
            self._dump_paths.objects_without_metadata if self._dump_paths else None,
            self._dump_paths.objects_not_found if self._dump_paths else None,
            self._dump_paths.metadata_files_unparsed if self._dump_paths else None,
            mode='w',
        ) as dump_files:
            yield dump_files

    def analyze(self, metadata_paths_to_analyze: Iterable[Path]) -> S3AnalyzerStats:
        with self.open_dump_files_if_needed() as (
            objects_with_metadata_dump,
            objects_without_metadata_dump,
            objects_not_found_dump,
            metadata_files_unparsed_dump,
        ):
            metadata_by_object_key = self._collect_metadata(metadata_paths_to_analyze, metadata_files_unparsed_dump)
            stats = self._analyze_s3_bucket(
                self._bucket,
                metadata_by_object_key,
                objects_with_metadata_dump,
                objects_without_metadata_dump,
                objects_not_found_dump,
            )

            return stats

    def _analyze_s3_bucket(
        self,
        bucket: Any,
        metadata_by_object_key: MetaDataByObjectKey,
        objects_with_metadata_dump: IO | None,
        objects_without_metadata_dump: IO | None,
        objects_not_found_dump: IO | None,
    ) -> S3AnalyzerStats:
        logging.debug(f'Start S3 bucket [{bucket.name}] analyzing...')
        for obj in bucket.objects.filter(Prefix=self._list_prefix):
            key = obj.key[len(self._base_prefix) :]
            metadata = metadata_by_object_key.get(key)

            if metadata:
                metadata.checked = True

                if objects_with_metadata_dump:
                    files = ', '.join(str(file) for file in metadata.files)
                    objects_with_metadata_dump.write(
                        f'S3 object: {obj.key} | Size: {obj.size:<10} | LastModified: {obj.last_modified} | '
                        f'Files: {files}\n'
                    )

                self._stats.existing_objects_with_metadata_count += 1
                self._stats.existing_objects_with_metadata_total_size += obj.size
            else:
                if objects_without_metadata_dump:
                    objects_without_metadata_dump.write(
                        f'S3 object: {obj.key} | Size: {obj.size:<10} | LastModified: {obj.last_modified}\n'
                    )

                self._stats.existing_objects_without_metadata_count += 1
                self._stats.existing_objects_without_metadata_total_size += obj.size

        for key, value in metadata_by_object_key.items():
            if not value.checked:
                self._stats.non_existing_objects_with_metadata_count += 1

                if objects_not_found_dump:
                    for file, meta in value.files.items():
                        s3_objects = ','.join(obj.key for obj in meta.objects)
                        objects_not_found_dump.write(f'File: {file} | S3 objects: {s3_objects}\n')

        logging.debug(f'S3 Bucket [{bucket.name}] analyzing is finished')
        return self._stats

    def _collect_metadata(self, paths: Iterable[Path], metadata_files_unparsed_dump: IO | None) -> MetaDataByObjectKey:
        """
        Return collection of parsed metadata from local disk with access by S3 object key.
        """
        logging.debug('Metadata collecting...')
        res: MetaDataByObjectKey = defaultdict(CheckedMetaData)

        for path in paths:
            if not path.exists():
                raise ValueError(f'Metadata path `{path}` is not exist')

        for file in chain.from_iterable(path.rglob('*') for path in paths):
            if not file.is_file():
                continue

            try:
                metadata = S3ObjectLocalMetaData.from_file(file)
            except Exception:
                self._stats.metadata_files_unparsed += 1

                if metadata_files_unparsed_dump:
                    metadata_files_unparsed_dump.write(f'File: {file}\n')
                continue

            for obj in metadata.objects:
                res[obj.key].files[file] = metadata

        logging.debug('Metadata collecting is finished')
        return res
