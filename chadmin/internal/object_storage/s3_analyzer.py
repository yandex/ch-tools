import logging

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from itertools import chain
from pathlib import Path
from typing import Any, Iterable

from cloud.mdb.clickhouse.tools.chadmin.internal.object_storage.dump_writer import DumpWriter
from cloud.mdb.clickhouse.tools.chadmin.internal.object_storage.s3_object_metadata import (
    CheckedMetaData,
    S3ObjectLocalMetaData,
)

IGNORED_PREFIXES = ['operations', '.SCHEMA_VERSION']

# The guard interval is used for S3 objects for which metadata is not found.
# And for metadata for which object is not found in S3.
# These objects are not counted if their last modified time fall in the interval from the moment of starting analyzing.
GUARD_INTERVAL = timedelta(hours=24)

MetaDataByObjectKey = dict[str, CheckedMetaData]


@dataclass
class CommonStats:
    count: int = 0
    total_size: int = 0
    dump_path: Path | None = None

    def inc(self, count: int = 1, size: int = 0) -> None:
        self.count += count
        self.total_size += size


@dataclass
class S3AnalyzerStats:
    objects_with_metadata: CommonStats = field(default_factory=CommonStats)
    objects_without_metadata: CommonStats = field(default_factory=CommonStats)
    not_found_objects_with_metadata: CommonStats = field(default_factory=CommonStats)
    unparsed_metadata: CommonStats = field(default_factory=CommonStats)
    ignored_objects: CommonStats = field(default_factory=CommonStats)


class S3Analyzer:
    def __init__(self, bucket: Any, dump_writer: DumpWriter, prefix: str = '', object_name_prefix: str = '') -> None:
        self._bucket = bucket
        self._dump_writer = dump_writer
        self._prefix = prefix
        self._object_name_prefix = object_name_prefix

        self._stats = S3AnalyzerStats()
        self._save_dump_paths_to_stats()

        self._pivot_time = datetime.now(timezone.utc) - GUARD_INTERVAL

    def analyze(self, metadata_paths_to_analyze: Iterable[Path]) -> S3AnalyzerStats:
        """
        Analyze objects in S3 and references to them in metadata and return statistics.
        """
        metadata_by_object_key = self._collect_metadata(metadata_paths_to_analyze)
        self._analyze_s3_bucket(self._bucket, metadata_by_object_key)
        self._collect_unchecked_keys(metadata_by_object_key)

        return self._stats

    def _analyze_s3_bucket(
        self,
        bucket: Any,
        metadata_by_object_key: MetaDataByObjectKey,
    ) -> None:
        """
        Run over S3 bucket objects and try to find its metadata.

        Update statistics and dump object keys if needed.
        """
        logging.debug(f'Start S3 bucket [{bucket.name}] analyzing...')

        for obj in bucket.objects.filter(Prefix=self._prefix + self._object_name_prefix):
            key: str = obj.key[len(self._prefix) :]

            if self._is_ignored(key):
                self._stats.ignored_objects.inc(1, obj.size)
                continue

            metadata = metadata_by_object_key.get(key)

            if metadata:
                metadata.checked = True

                self._dump_writer.write_object_with_metadata(obj, metadata)
                self._stats.objects_with_metadata.inc(1, obj.size)

            elif obj.last_modified <= self._pivot_time:
                self._dump_writer.write_object_without_metadata(obj)
                self._stats.objects_without_metadata.inc(1, obj.size)

        logging.debug(f'S3 Bucket [{bucket.name}] analyzing is finished')

    def _collect_unchecked_keys(self, metadata_by_object_key: MetaDataByObjectKey) -> None:
        """
        Find S3 object keys that are referenced in the metadata but not found in S3 bucket.

        They have not been checked during running over all S3 objects in the course of analysing.
        """
        for key, metadata in metadata_by_object_key.items():
            if metadata.checked:
                continue

            for file, meta in metadata.files.items():
                metadata_file_last_modified = datetime.fromtimestamp(file.stat().st_mtime, timezone.utc)

                if metadata_file_last_modified <= self._pivot_time:
                    self._stats.not_found_objects_with_metadata.inc()
                    self._dump_writer.write_not_found_object(key, metadata)
                    break

    def _collect_metadata(self, paths: Iterable[Path]) -> MetaDataByObjectKey:
        """
        Return dictionary of parsed metadata from local disk with access by S3 object key.
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
                self._stats.unparsed_metadata.inc(1, file.stat().st_size)
                self._dump_writer.write_unparsed_metadata(str(file))
                continue

            for obj in metadata.objects:
                res[obj.key].files[file] = metadata

        logging.debug('Metadata collecting is finished')
        return res

    def _save_dump_paths_to_stats(self) -> None:
        self._stats.objects_without_metadata.dump_path = self._dump_writer.objects_without_metadata_dump_path
        self._stats.objects_with_metadata.dump_path = self._dump_writer.objects_with_metadata_dump_path
        self._stats.not_found_objects_with_metadata.dump_path = self._dump_writer.not_found_objects_dump_path
        self._stats.unparsed_metadata.dump_path = self._dump_writer.unparsed_metadata_dump_path

    @staticmethod
    def _is_ignored(key: str) -> bool:
        return any(key.startswith(p) for p in IGNORED_PREFIXES)
