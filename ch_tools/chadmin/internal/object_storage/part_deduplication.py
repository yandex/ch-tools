"""Deduplication records belonging to selected part ranges."""

from __future__ import annotations

import posixpath
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

from ch_tools.common import logging

DEDUPLICATION_DIRECTORIES = ("deduplication_hashes", "blocks", "async_blocks")
PART_RANGE_RE = re.compile(
    r"([A-Za-z0-9-]+)_(-?[0-9]+)_(-?[0-9]+)_[0-9]+(?:_[0-9]+)?\Z"
)
LEGACY_PART_RANGE_RE = re.compile(
    r"([0-9]{8})_([0-9]{8})_(-?[0-9]+)_(-?[0-9]+)_[0-9]+(?:_[0-9]+)?\Z"
)


@dataclass(frozen=True)
class PartRange:
    partition_id: str
    first: int
    last: int

    @classmethod
    def parse(cls, name: str, format_version: int) -> PartRange:
        pattern = LEGACY_PART_RANGE_RE if format_version == 0 else PART_RANGE_RE
        match = pattern.fullmatch(name)
        if match is None:
            raise ValueError(f"Cannot determine the block range of part {name!r}")
        if format_version == 0:
            min_date, max_date, first, last = match.groups()
            if min_date[:6] != max_date[:6] or min_date > max_date:
                raise ValueError(f"Invalid date range in part {name!r}")
            partition = min_date[:6]
        else:
            partition, first, last = match.groups()
        result = cls(partition, int(first), int(last))
        if result.first > result.last:
            raise ValueError(f"Invalid block range in part {name!r}")
        return result

    def contains(self, other: PartRange) -> bool:
        return (
            self.partition_id == other.partition_id
            and self.first <= other.first
            and other.last <= self.last
        )


@dataclass(frozen=True)
class DeduplicationNode:
    path: str
    part_name: str


def _part_format_version(zk: KazooClient, root: str) -> int:
    # On object-storage disks, `format_version.txt` can itself be an S3 stub.
    path = posixpath.join(root, "metadata")
    data, _ = zk.get(path)
    metadata = data.decode("utf-8")
    if metadata.startswith("metadata format version: 1\n"):
        date_columns = re.findall(r"^date column: (.*)$", metadata, re.MULTILINE)
        versions = re.findall(r"^data format version: (.*)$", metadata, re.MULTILINE)
        if len(date_columns) == 1:
            if date_columns[0] and not versions:
                return 0
            if not date_columns[0] and versions == ["1"]:
                return 1
    raise ValueError(
        f"Cannot safely determine the part format from Keeper metadata: {path}"
    )


def _deduplication_directories(zk: KazooClient, root: str) -> Dict[str, List[str]]:
    result = {}
    for directory in DEDUPLICATION_DIRECTORIES:
        path = posixpath.join(root, directory)
        try:
            names = zk.get_children(path)
        except NoNodeError:
            # Older servers do not create every deduplication directory.
            continue
        if names:
            result[path] = names
    return result


def collect_deduplication_nodes(
    zk: KazooClient, root: str, part_names: Iterable[str]
) -> Dict[str, List[DeduplicationNode]]:
    """Match stored part ranges, rather than interpreting hash or token names."""
    result: Dict[str, List[DeduplicationNode]] = {name: [] for name in part_names}
    directories = _deduplication_directories(zk, root)
    if not result or not directories:
        return result
    format_version = _part_format_version(zk, root)
    # Selected parts grouped by partition:
    # `{"all": [("all_1_3_2", PartRange("all", 1, 3))]}`.
    ranges: Dict[str, List[tuple[str, PartRange]]] = {}
    for name in result:
        info = PartRange.parse(name, format_version)
        ranges.setdefault(info.partition_id, []).append((name, info))
    for directory_path, names in directories.items():
        for name in sorted(names):
            if name.split("_", 1)[0] not in ranges:
                continue
            path = posixpath.join(directory_path, name)
            try:
                data, stat = zk.get(path)
            except NoNodeError:
                continue  # Background cleanup may already have removed it.
            part_name = data.decode("utf-8")
            try:
                info = PartRange.parse(part_name, format_version)
            except ValueError:
                logging.warning(
                    "Retaining deduplication node {}: unrecognized part value {!r}",
                    path,
                    part_name,
                )
                continue
            matches = [
                selected_name
                for selected_name, selected in ranges.get(info.partition_id, [])
                if selected.contains(info) and name.startswith(f"{info.partition_id}_")
            ]
            if not matches:
                continue
            if stat.children_count:
                raise ValueError(f"Deduplication node is not a leaf: {path}")
            node = DeduplicationNode(path, part_name)
            for selected_name in matches:
                result[selected_name].append(node)
    return result
