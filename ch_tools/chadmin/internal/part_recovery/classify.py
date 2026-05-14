"""
Classification of files in a MergeTree part (Wide or Compact format) stored on an S3 disk.

Each file in the part directory is a local metadata file pointing to one or more
S3 blobs.  This module inspects the file names, determines the semantic role of
each file, and decides what to do when its S3 blobs are missing.

For details on Compact vs Wide part handling, see :func:`classify`.
"""

import logging
import os
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    S3ObjectLocalInfo,
    S3ObjectLocalMetaData,
)
from ch_tools.chadmin.internal.part_recovery.exceptions import (
    CriticalLossError,
)
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration


class FileCategory(str, Enum):
    REQUIRED_META = "required_meta"  # columns.txt, count.txt, partition.dat
    META = "meta"
    DATA = "data"
    MARK = "mark"
    SKIP_INDEX = "skip_index"
    INDEX = "index"
    UNKNOWN = "unknown"


class Decision(str, Enum):
    DOWNLOAD = "download"
    ZERO_FILL = "zero_fill"
    DROP = "drop"
    RECONSTRUCT = "reconstruct"
    MARK_BROKEN = "mark_broken"
    KEEP = "keep"


# Patterns for recognizing file roles by name.
# Supports both legacy (.bin/.mrk2) and new ClickHouse formats (.cmrk2/.size.bin etc.)
# Use non-greedy .+? so that "value.size.bin" → col="value", not "value.size".
_RE_DATA = re.compile(r"^(?P<col>.+?)\.(bin|size\.bin)$")
_RE_MARK2 = re.compile(r"^(?P<col>.+?)\.(mrk2|cmrk2|size\.mrk2|size\.cmrk2)$")
_RE_MARK = re.compile(r"^(?P<col>.+?)\.mrk$")
_RE_MINMAX = re.compile(r"^minmax_(?P<col>.+?)\.idx$")
_RE_SKP_IDX = re.compile(r"^skp_idx_(?P<name>.+?)\.(idx|mrk2?|cmrk2|idx2)$")
_RE_PRIMARY = re.compile(r"^primary\.(idx|cidx)$")
_RE_COMPACT_DATA = re.compile(r"^data\.bin$")

# Files that are always dropped regardless of blob health
_ALWAYS_DROP_NAMES = {"checksums.txt"}
_ALWAYS_DROP_CATEGORIES = {FileCategory.SKIP_INDEX}

# Plain-text meta files that map to FileCategory.META
_META_PLAIN_NAMES = {
    "partition.dat",
    "default_compression_codec.txt",
    "metadata_version.txt",
    "serialization.json",
    "ttl.txt",
    "uuid.txt",
    "checksums.txt",
}

# Required meta files that cannot be zero-filled if their S3 blobs are missing.
# These are handled specially by the orchestrator (e.g. columns.txt reconstruction).
# - columns.txt: can be reconstructed from another part or system.columns
# - partition.dat: plain-text file (no S3 blobs), must exist locally
# - count.txt: can be reconstructed from healthy .mrk2 files
_REQUIRED_META_NAMES = {"columns.txt", "count.txt", "partition.dat"}


@dataclass
class PartFile:
    """Represents a single file inside a MergeTree part directory (Wide or Compact)."""

    name: str
    path: Path
    metadata: Optional[S3ObjectLocalMetaData]
    category: FileCategory
    column: Optional[str]
    healthy: bool
    decision: Decision
    blob_keys: List[str] = field(default_factory=list)
    total_size: int = 0


def _full_key(disk_conf: S3DiskConfiguration, obj: S3ObjectLocalInfo) -> str:
    """Return the full S3 key for a blob object, prepending the disk prefix if needed."""
    if obj.key_is_full:
        return obj.key
    return os.path.join(disk_conf.prefix, obj.key)


def _categorize(name: str) -> tuple:  # pylint: disable=too-many-return-statements
    """
    Return (FileCategory, column_name_or_None) for a given file name.
    """
    # Required meta files — check first to avoid false matches by data regexes
    if name in _REQUIRED_META_NAMES:
        return FileCategory.REQUIRED_META, None

    # Compact part sentinel — handled separately in classify()
    if _RE_COMPACT_DATA.match(name):
        return FileCategory.DATA, None

    if _RE_SKP_IDX.match(name):
        return FileCategory.SKIP_INDEX, None

    m = _RE_DATA.match(name)
    if m:
        return FileCategory.DATA, m.group("col")

    m = _RE_MARK2.match(name) or _RE_MARK.match(name)
    if m:
        return FileCategory.MARK, m.group("col")

    if _RE_MINMAX.match(name) or name in _META_PLAIN_NAMES:
        return FileCategory.META, None

    # New ClickHouse format: columns_substreams.txt
    if name == "columns_substreams.txt":
        return FileCategory.META, None

    if _RE_PRIMARY.match(name):
        return FileCategory.INDEX, None

    return FileCategory.UNKNOWN, None


def _decide(
    category: FileCategory,
    name: str,
    healthy: bool,
    has_healthy_mrk2: bool,
    has_metadata: bool = True,
) -> Decision:
    """
    Determine what to do with a file given its category and blob health.

    Parameters
    ----------
    has_metadata:
        True if the file is an S3 metadata file (has blobs to download).
        False if it is a plain-text local file (already on disk, no S3 blobs).
    """
    # Always drop skip-indexes and checksums.txt regardless of health
    if category in _ALWAYS_DROP_CATEGORIES or name in _ALWAYS_DROP_NAMES:
        return Decision.DROP

    if healthy:
        # Plain-text local files (no S3 metadata) are kept as-is — nothing to download.
        # S3 metadata files must be downloaded even for CRITICAL_META (count.txt, columns.txt).
        return Decision.DOWNLOAD if has_metadata else Decision.KEEP

    # Blob(s) missing — decide by category:
    if category == FileCategory.REQUIRED_META:
        if name == "count.txt" and has_healthy_mrk2:
            return Decision.RECONSTRUCT
        # columns.txt missing → unrecoverable (CriticalLossError raised by caller)
        return Decision.MARK_BROKEN

    if category in (FileCategory.META, FileCategory.INDEX):
        return Decision.ZERO_FILL

    if category in (FileCategory.DATA, FileCategory.MARK):
        return Decision.MARK_BROKEN

    return Decision.DROP


def _collect_healthy_mrk2_columns(
    all_paths: List[Path],
    blob_status: Dict[str, bool],
    disk_conf: Optional[S3DiskConfiguration],
) -> set:
    """
    Collect column names that have at least one healthy .mrk2 file.

    This is used to determine whether count.txt can be reconstructed.
    """
    healthy_mrk2_cols: set = set()
    for path in all_paths:
        match = _RE_MARK2.match(path.name)
        if match:
            try:
                meta = S3ObjectLocalMetaData.from_file(path)
                all_healthy = all(
                    blob_status.get(
                        _full_key(disk_conf, obj) if disk_conf else obj.key, False
                    )
                    for obj in meta.objects
                )
                if all_healthy:
                    healthy_mrk2_cols.add(match.group("col"))
            except (ValueError, IndexError, OSError) as exc:
                logging.debug("Failed to parse metadata for %s: %s", path.name, exc)
    return healthy_mrk2_cols


def scan_blob_keys(
    part_dir: Path,
    disk_conf: Optional[S3DiskConfiguration] = None,
) -> Dict[str, str]:
    """
    Scan *part_dir* and return a mapping ``{full_blob_key: file_name}`` for
    every S3 blob referenced by any metadata file in the directory.

    This is a lightweight scan that does **not** validate blob health or raise
    any exceptions — it is used to collect the set of keys to check before
    calling :func:`classify`.
    """
    keys: Dict[str, str] = {}
    for path in part_dir.iterdir():
        if not path.is_file():
            continue
        try:
            meta = S3ObjectLocalMetaData.from_file(path)
            for obj in meta.objects:
                full = _full_key(disk_conf, obj) if disk_conf else obj.key
                keys[full] = path.name
        except (ValueError, IndexError, OSError) as exc:
            logging.debug("Failed to parse metadata for %s: %s", path.name, exc)
    return keys


def classify(
    part_dir: Path,
    blob_status: Dict[str, bool],
    disk_conf: Optional[S3DiskConfiguration] = None,
) -> List[PartFile]:
    """
    Scan *part_dir* and classify every file.

    Supports both **Wide** and **Compact** MergeTree part formats.

    - **Compact** parts are identified by the presence of a ``data.bin`` file.
      All column data is stored in this single file. If ``data.bin`` has missing
      S3 blobs, recovery is impossible and :class:`CriticalLossError` is raised.
    - **Wide** parts have per-column ``.bin`` files. Individual broken columns
      can be excluded from recovery.
    - ``columns.txt`` may be missing — it will be reconstructed later by the orchestrator
      from another part or from ``system.columns``.

    Raises
    ------
    CriticalLossError:
        If the part is in Compact format and ``data.bin`` has missing S3 blobs,
        or if any required metadata file (e.g. ``partition.dat``) has missing blobs.
    """

    files: List[PartFile] = []

    # Detect Compact vs Wide format
    compact_data = part_dir / "data.bin"
    is_compact = compact_data.exists()

    # Collect all files
    all_paths = [p for p in part_dir.iterdir() if p.is_file()]

    # Determine which columns have at least one healthy .mrk2 file
    # (needed to decide whether count.txt can be reconstructed)
    has_healthy_mrk2 = (
        len(_collect_healthy_mrk2_columns(all_paths, blob_status, disk_conf)) > 0
    )

    for path in sorted(all_paths, key=lambda p: p.name):
        name = path.name
        category, column = _categorize(name)

        # Try to parse as S3 metadata
        metadata: Optional[S3ObjectLocalMetaData] = None
        blob_keys: List[str] = []
        total_size = 0

        try:
            metadata = S3ObjectLocalMetaData.from_file(path)
            # Store full keys so callers can look them up in blob_status directly
            blob_keys = [
                _full_key(disk_conf, obj) if disk_conf else obj.key
                for obj in metadata.objects
            ]
            total_size = metadata.total_size
        except (ValueError, IndexError, OSError):
            # Plain text file (columns.txt, count.txt, checksums.txt, …)
            # or unrecognized binary — treat as locally present, no S3 blobs.
            metadata = None

        if metadata is not None:
            healthy = all(blob_status.get(k, False) for k in blob_keys)
        else:
            # Local plain-text file — always "healthy" (present on disk)
            healthy = True

        decision = _decide(
            category, name, healthy, has_healthy_mrk2, has_metadata=metadata is not None
        )

        files.append(
            PartFile(
                name=name,
                path=path,
                metadata=metadata,
                category=category,
                column=column,
                healthy=healthy,
                decision=decision,
                blob_keys=blob_keys,
                total_size=total_size,
            )
        )

    # Note: columns.txt may be missing — it will be reconstructed later
    # from another part or from system.columns by the orchestrator.

    # Validate: any MARK_BROKEN on a REQUIRED_META file → fail
    for pf in files:
        if (
            pf.category == FileCategory.REQUIRED_META
            and pf.decision == Decision.MARK_BROKEN
        ):
            raise CriticalLossError(
                f"Critical metadata file '{pf.name}' has missing S3 blobs "
                "and cannot be reconstructed. Recovery is impossible."
            )

    # Compact part validation: data.bin must be healthy
    if is_compact:
        compact_file = next((pf for pf in files if pf.name == "data.bin"), None)
        if compact_file and not compact_file.healthy:
            raise CriticalLossError(
                f"Compact part at {part_dir} has missing S3 blobs in data.bin. "
                "All column data is stored in a single file; recovery is impossible."
            )

    return files
