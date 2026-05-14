"""
Recovery report: collects statistics and writes a JSON summary.
"""

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from ch_tools.chadmin.internal.part_recovery.classify import (
    Decision,
    PartFile,
)


@dataclass
class MissingBlobInfo:
    file: str
    category: str
    decision: str
    size: int
    blob_keys: List[str]


@dataclass
class RecoveryReport:
    part_path: str
    files_total: int = 0
    files_healthy: int = 0
    files_missing: int = 0
    missing_blobs: List[MissingBlobInfo] = field(default_factory=list)
    broken_columns: List[str] = field(default_factory=list)
    reconstructed_files: List[str] = field(default_factory=list)
    dropped_files: List[str] = field(default_factory=list)
    rows_recovered: Optional[int] = None
    tsv_columns: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)

    def write(self, path: Path) -> None:
        path.write_text(self.to_json(), encoding="utf-8")


def build_report(
    part_path: Path,
    part_files: List[PartFile],
    rows_recovered: Optional[int] = None,
    tsv_columns: Optional[List[str]] = None,
) -> RecoveryReport:
    """
    Build a :class:`RecoveryReport` from the classified *part_files*.
    """
    report = RecoveryReport(part_path=str(part_path))
    report.rows_recovered = rows_recovered
    report.tsv_columns = tsv_columns or []

    for pf in part_files:
        # Only count files that have S3 metadata (skip plain local files)
        if pf.metadata is None:
            continue

        report.files_total += 1

        if pf.healthy:
            report.files_healthy += 1
        else:
            report.files_missing += 1
            report.missing_blobs.append(
                MissingBlobInfo(
                    file=pf.name,
                    category=pf.category.value,
                    decision=pf.decision.value,
                    size=pf.total_size,
                    blob_keys=pf.blob_keys,
                )
            )

        if pf.decision == Decision.MARK_BROKEN and pf.column:
            if pf.column not in report.broken_columns:
                report.broken_columns.append(pf.column)

        if pf.decision == Decision.RECONSTRUCT:
            report.reconstructed_files.append(pf.name)

        if pf.decision == Decision.DROP:
            report.dropped_files.append(pf.name)

    # Also account for reconstructed plain-text files (count.txt, checksums.txt)
    # that are not S3 metadata files — they are tracked separately by the orchestrator.

    return report
