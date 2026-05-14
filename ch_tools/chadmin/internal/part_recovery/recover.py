"""
Orchestrator for recovering a broken MergeTree part (Wide or Compact format) with missing S3 blobs.

High-level flow
---------------
1. Load S3DiskConfiguration from ClickHouse config.
2. Scan the part directory, parse S3 metadata files, and collect full blob keys.
3. Check all referenced S3 blobs in parallel (head_object).
4. Classify every file and decide what to do with it.
   - For **Compact** parts: if ``data.bin`` has missing blobs, :class:`CriticalLossError` is raised.
   - For **Wide** parts: individual broken columns can be excluded from recovery.
5. (dry-run) Print the plan and exit.
6. Download healthy blobs into a temporary directory.
7. Reconstruct missing meta files (zero-fill, stubs, count.txt).
8. Use the running ClickHouse server to extract data:
   a. Create a scratch table ``_chadmin_recover._recover_<rand>`` on a local disk.
   b. Place the assembled part in the scratch table's detached/ directory.
   c. ATTACH PART → SELECT FORMAT TSV → output file.
   d. DROP scratch table (always, in finally).
9. Write the JSON report.
"""

import re
import shutil
import tempfile
from pathlib import Path
from typing import List, Optional, Set

from ch_tools.chadmin.internal.part_recovery.classify import (
    Decision,
    FileCategory,
    PartFile,
    classify,
    scan_blob_keys,
)
from ch_tools.chadmin.internal.part_recovery.reconstruct import (
    count_rows_from_mrk2,
    reconstruct_missing_meta,
    write_count_txt,
    zero_fill,
)
from ch_tools.chadmin.internal.part_recovery.report import RecoveryReport, build_report
from ch_tools.chadmin.internal.part_recovery.s3_blobs import (
    check_blobs_parallel,
    download_part_files,
)
from ch_tools.chadmin.internal.part_recovery.server_runner import (
    reconstruct_columns_txt,
    run_server_recovery,
)
from ch_tools.common import logging
from ch_tools.common.clickhouse.client.clickhouse_client import ClickhouseClient
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration


def recover_broken_part(
    client: ClickhouseClient,
    disk_conf: S3DiskConfiguration,
    part_path: Path,
    output_tsv: Path,
    tmp_dir: Optional[Path] = None,
    threads: int = 16,
    dry_run: bool = False,
    report_path: Optional[Path] = None,
) -> RecoveryReport:
    """
    Recover a broken MergeTree part (Wide or Compact format) and export readable rows to TSV.

    - **Compact** parts: all column data is stored in a single ``data.bin`` file.
      If ``data.bin`` has missing S3 blobs, :class:`CriticalLossError` is raised.
    - **Wide** parts: each column has its own ``.bin`` file. Individual broken
      columns can be excluded from recovery (replaced with NULL in output).
    - ``columns.txt`` may be missing — it will be reconstructed from another part
      or from ``system.columns``.

    Raises
    ------
    CriticalLossError:
        If the part is in Compact format and ``data.bin`` has missing S3 blobs,
        or if any required metadata file (e.g. ``partition.dat``) has missing blobs,
        or if ``columns.txt`` cannot be reconstructed.
    """

    part_name = part_path.name
    logging.info("Starting recovery of part '{}' at {}", part_name, part_path)

    # ── Step 1: Scan part directory to collect all blob keys ──────────────────
    logging.info("Scanning part directory for S3 blob keys …")
    all_blob_keys = set(scan_blob_keys(part_path, disk_conf=disk_conf).keys())

    # ── Step 2: Check S3 blobs in parallel ────────────────────────────────────
    logging.info("Checking {} unique S3 blob keys …", len(all_blob_keys))
    blob_status = check_blobs_parallel(disk_conf, all_blob_keys, threads=threads)

    # ── Step 3: Classify files with actual blob health status ─────────────────
    logging.info("Classifying part files …")
    part_files = classify(part_path, blob_status=blob_status, disk_conf=disk_conf)

    # ── Step 4: Determine broken columns ─────────────────────────────────────
    # For Compact parts, all data is in data.bin which is either fully healthy
    # or causes CriticalLossError. So broken_columns is always empty for Compact.
    is_compact = (part_path / "data.bin").exists()
    broken_columns: Set[str] = set()
    if not is_compact:
        for pf in part_files:
            if pf.decision == Decision.MARK_BROKEN and pf.column:
                broken_columns.add(pf.column)

    _log_plan(part_files, broken_columns, is_compact=is_compact)

    # ── Step 6: dry-run exit ──────────────────────────────────────────────────
    if dry_run:
        logging.info("Dry-run mode: no files will be modified.")
        report = build_report(part_path, part_files)
        if report_path:
            report.write(report_path)
            logging.info("Report written to {}", report_path)
        return report

    # ── Step 7: Set up temporary working directory ────────────────────────────
    own_tmp = tmp_dir is None
    if tmp_dir is None:
        work_dir = Path(tempfile.mkdtemp(prefix="ch-tools-recovery-"))
    else:
        work_dir = tmp_dir / f"recovery_{part_name}"
        work_dir.mkdir(parents=True, exist_ok=True)

    try:
        assembled_dir = work_dir / "parts" / part_name
        assembled_dir.mkdir(parents=True, exist_ok=True)

        # ── Step 8: Download healthy blobs ────────────────────────────────────
        logging.info("Downloading healthy blobs …")
        download_part_files(disk_conf, part_files, assembled_dir, threads=threads)

        # ── Step 9: Copy plain-text local files ───────────────────────────────
        _copy_local_files(assembled_dir, part_files)

        # ── Step 9a: Reconstruct missing meta files ───────────────────────────
        reconstructed: List[str] = []
        _reconstruct_files(assembled_dir, part_files, reconstructed)

        # ── Step 9b: Reconstruct missing columns.txt from another part or
        #              system.columns (both Wide and Compact formats)
        if not (assembled_dir / "columns.txt").exists():
            columns_txt_source = reconstruct_columns_txt(
                client=client,
                part_path=part_path,
                assembled_dir=assembled_dir,
            )
            reconstructed.append(f"columns.txt (from {columns_txt_source})")

        # ── Step 9c: Strip broken columns from columns.txt /
        #              columns_substreams.txt so ATTACH PART does not look for
        #              their (deleted) .bin/.mrk2 files.
        # This is only needed for Wide parts; Compact parts never have broken_columns.
        if broken_columns and not is_compact:
            _strip_broken_from_columns_files(
                assembled_dir, broken_columns, reconstructed
            )

        # ── Step 10: Determine columns.txt path ───────────────────────────────
        columns_txt = assembled_dir / "columns.txt"
        assert (
            columns_txt.exists()
        ), f"columns.txt must exist after reconstruction for part {part_path.name}"

        # ── Step 12: Run server-side recovery ─────────────────────────────────
        logging.info("Running server-side recovery …")
        rows, tsv_labels = run_server_recovery(
            client=client,
            part_path=part_path,
            assembled_dir=assembled_dir,
            broken_columns=broken_columns,
            output_path=output_tsv,
            columns_txt=columns_txt,
        )

        # ── Step 13: Build and write report ───────────────────────────────────
        report = build_report(
            part_path, part_files, rows_recovered=rows, tsv_columns=tsv_labels
        )
        report.reconstructed_files = _deduplicate_preserving_order(
            [*report.reconstructed_files, *reconstructed]
        )

        if report_path:
            report.write(report_path)
            logging.info("Report written to {}", report_path)

        logging.info(
            "Recovery complete. {} rows written to {}. Broken columns: {} ({} part)",
            rows,
            output_tsv,
            sorted(broken_columns) or "none",
            "Compact" if is_compact else "Wide",
        )
        return report

    finally:
        if own_tmp:
            shutil.rmtree(str(work_dir), ignore_errors=True)


# ── Internal helpers ──────────────────────────────────────────────────────────


def _log_plan(
    part_files: List[PartFile], broken_columns: Set[str], is_compact: bool = False
) -> None:
    """Log a human-readable summary of the recovery plan."""
    total = len(part_files)
    healthy = sum(1 for pf in part_files if pf.healthy)
    missing = total - healthy
    part_format = "Compact" if is_compact else "Wide"
    logging.info(
        "Part files ({} format): {} total, {} healthy, {} with missing blobs",
        part_format,
        total,
        healthy,
        missing,
    )
    if is_compact:
        logging.info(
            "Compact part: all column data is in data.bin (no per-column recovery)."
        )
    elif broken_columns:
        logging.warning(
            "Broken columns (will be NULL in output): {}", sorted(broken_columns)
        )
    for pf in part_files:
        if not pf.healthy:
            logging.info(
                "  {} [{}] → {}",
                pf.name,
                pf.category.value,
                pf.decision.value,
            )


def _copy_local_files(
    assembled_dir: Path,
    part_files: List[PartFile],
) -> None:
    """
    Copy plain-text local files (columns.txt, count.txt, etc.) that are
    already present on disk (no S3 metadata) into the assembled directory.
    """
    for pf in part_files:
        if pf.metadata is not None:
            continue  # S3 metadata file — handled by download
        src = pf.path
        dst = assembled_dir / pf.name
        if src.exists() and not dst.exists():
            shutil.copy2(str(src), str(dst))


def _reconstruct_files(
    assembled_dir: Path,
    part_files: List[PartFile],
    reconstructed: List[str],
) -> None:
    """
    Apply reconstruction decisions: zero-fill, stubs, count.txt from mrk2.
    Also always drop checksums.txt so ClickHouse recalculates it on ATTACH.
    """
    # Always remove checksums.txt — ClickHouse will recalculate with force_restore_data=1
    checksums = assembled_dir / "checksums.txt"
    if checksums.exists():
        checksums.unlink()
        reconstructed.append("checksums.txt (dropped)")

    for pf in part_files:
        dest = assembled_dir / pf.name

        if pf.decision == Decision.DROP:
            if dest.exists():
                dest.unlink()
            reconstructed.append(f"{pf.name} (dropped)")
            continue

        if pf.decision == Decision.ZERO_FILL:
            size = pf.total_size if pf.total_size > 0 else 0
            zero_fill(dest, size)
            reconstructed.append(f"{pf.name} (zero-filled, {size} bytes)")
            continue

        if pf.decision == Decision.RECONSTRUCT and pf.name == "count.txt":
            # Find any healthy .mrk2 file in the assembled directory
            row_count = _count_rows_from_any_mrk2(assembled_dir)
            if row_count is not None:
                write_count_txt(dest, row_count)
                reconstructed.append(f"count.txt (reconstructed, {row_count} rows)")
            else:
                logging.warning(
                    "Could not reconstruct count.txt: no healthy .mrk2 files found. "
                    "Skipping — ClickHouse will handle it."
                )
            continue

        if pf.decision == Decision.MARK_BROKEN:
            # Data/mark file with missing blobs — drop it entirely.
            # Zero-filled .bin files would crash ATTACH PART with
            # UNKNOWN_CODEC because ClickHouse reads codec byte 0x00.
            # The owning column will also be excluded from the scratch
            # table schema, so the file will not be referenced.
            if dest.exists():
                dest.unlink()
            reconstructed.append(f"{pf.name} (dropped — column marked broken)")
            continue

        # For META files that are missing and not yet handled
        if pf.category == FileCategory.META and not dest.exists():
            reconstruct_missing_meta(assembled_dir, pf.name, pf.total_size)
            reconstructed.append(f"{pf.name} (stub)")


def _strip_broken_from_columns_files(
    assembled_dir: Path,
    broken_columns: Set[str],
    reconstructed: List[str],
) -> None:
    """
    Rewrite ``columns.txt`` (and ``columns_substreams.txt`` if present) in
    *assembled_dir* removing any references to *broken_columns*.

    This keeps ClickHouse from looking for the corresponding ``.bin`` /
    ``.mrk2`` files during ``ATTACH PART`` — those files have been deleted
    by the MARK_BROKEN handler, and the owning columns are also excluded
    from the scratch table schema.
    """
    columns_txt = assembled_dir / "columns.txt"
    if columns_txt.exists():
        _rewrite_columns_txt(columns_txt, broken_columns, reconstructed)

    substreams = assembled_dir / "columns_substreams.txt"
    if substreams.exists():
        _rewrite_columns_substreams_txt(substreams, broken_columns, reconstructed)

    # serialization.json (CH 23.x+) is an optional optimisation that lists
    # per-column serialization info (e.g. for Sparse columns).  When we
    # remove columns from the part, ClickHouse rejects the part with
    # "Found unexpected column ... in serialization infos".  Dropping the
    # file is safe — ClickHouse falls back to default serialization.
    serialization = assembled_dir / "serialization.json"
    if serialization.exists():
        serialization.unlink()
        reconstructed.append("serialization.json (dropped)")


def _rewrite_columns_txt(
    path: Path,
    broken_columns: Set[str],
    reconstructed: List[str],
) -> None:
    """Drop ``broken_columns`` from a ``columns.txt`` file in place."""
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()

    header: List[str] = []
    body: List[str] = []
    body_started = False
    for line in lines:
        if not body_started and re.match(r"^`", line.strip()):
            body_started = True
        if body_started:
            body.append(line)
        else:
            header.append(line)

    kept: List[str] = []
    for line in body:
        m = re.match(r"^\s*`(.+?)`\s+", line)
        if m and m.group(1) in broken_columns:
            continue
        kept.append(line)

    # Update the column-count header line ("N columns:") if it exists
    new_header: List[str] = []
    for line in header:
        m = re.match(r"^\s*(\d+)\s+columns:\s*$", line)
        if m:
            new_header.append(f"{len(kept)} columns:")
        else:
            new_header.append(line)

    path.write_text("\n".join(new_header + kept) + "\n", encoding="utf-8")
    reconstructed.append(f"columns.txt (rewritten, dropped {sorted(broken_columns)})")


def _rewrite_columns_substreams_txt(
    path: Path,
    broken_columns: Set[str],
    reconstructed: List[str],
) -> None:
    """
    Drop column blocks that belong to *broken_columns* from
    ``columns_substreams.txt`` (CH 26.x).

    Format::

        columns substreams version: 1
        N columns:
        K substreams for column `name`:
            <substream1>
            <substream2>
        ...
    """
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()

    header: List[str] = []
    body_blocks: List[List[str]] = []
    current_block: List[str] = []
    body_started = False

    for line in lines:
        m_block = re.match(
            r"^\s*\d+\s+substreams\s+for\s+column\s+`(.+?)`\s*:\s*$", line
        )
        if m_block:
            body_started = True
            if current_block:
                body_blocks.append(current_block)
            current_block = [line]
            continue
        if not body_started:
            header.append(line)
        else:
            current_block.append(line)
    if current_block:
        body_blocks.append(current_block)

    kept_blocks: List[List[str]] = []
    for block in body_blocks:
        m = re.match(r"^\s*\d+\s+substreams\s+for\s+column\s+`(.+?)`\s*:\s*$", block[0])
        col = m.group(1) if m else ""
        if col in broken_columns:
            continue
        kept_blocks.append(block)

    # Update the "N columns:" line in header
    new_header: List[str] = []
    for line in header:
        m = re.match(r"^\s*(\d+)\s+columns:\s*$", line)
        if m:
            new_header.append(f"{len(kept_blocks)} columns:")
        else:
            new_header.append(line)

    out_lines = list(new_header)
    for block in kept_blocks:
        out_lines.extend(block)

    path.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
    reconstructed.append(
        f"columns_substreams.txt (rewritten, dropped {sorted(broken_columns)})"
    )


def _deduplicate_preserving_order(items: List[str]) -> List[str]:
    """Return *items* without duplicates, preserving the first occurrence."""
    seen: Set[str] = set()
    result: List[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _count_rows_from_any_mrk2(assembled_dir: Path) -> Optional[int]:
    """
    Try to read row count from any mark file in *assembled_dir*.
    Returns the count from the first successfully parsed file, or None.
    Files are sorted for deterministic selection.
    """
    patterns = ("*.mrk2", "*.cmrk2", "*.size.mrk2", "*.size.cmrk2")
    candidates: List[Path] = []
    for pattern in patterns:
        candidates.extend(assembled_dir.glob(pattern))

    for p in sorted(set(candidates)):
        try:
            return count_rows_from_mrk2(p)
        except (ValueError, OSError):
            continue
    return None
