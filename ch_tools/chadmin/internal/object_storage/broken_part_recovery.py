"""Recover intact columns from one broken detached S3 data part.

The recovery flow inspects ClickHouse object-storage metadata, builds a valid
staging part from complete streams, and copies the readable data into a new
MergeTree table.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Set

import boto3
from botocore.client import Config
from click import Context

from ch_tools.chadmin.internal.clickhouse_disks import ClickHouseDiskClient
from ch_tools.chadmin.internal.object_storage.part_recovery_metadata import (
    ROW_EXISTS_COLUMN,
    PartColumn,
    RecoveryAnalysis,
    filter_serialization,
    parse_columns_substreams,
    parse_columns_text,
    render_columns_substreams,
    render_columns_text,
    validate_columns_substreams,
)
from ch_tools.chadmin.internal.object_storage.part_recovery_source import (
    DiskInfo,
    RecoverySource,
    TableRef,
    inspect_file_recoverability,
    parse_qualified_table,
    quote_identifier,
    quote_string,
    resolve_recovery_source,
    restore_missing_empty_objects,
)
from ch_tools.chadmin.internal.part import attach_part, list_parts
from ch_tools.chadmin.internal.system import get_version
from ch_tools.chadmin.internal.table import check_table, delete_table, table_exists
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.clickhouse.config import get_clickhouse_config
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration
from ch_tools.common.utils import escape_for_file_name, version_ge

# ClickHouse structural metadata read or regenerated for the staging part.
COLUMNS_FILE = "columns.txt"
COLUMNS_SUBSTREAMS_FILE = "columns_substreams.txt"
COUNT_FILE = "count.txt"
DEFAULT_COMPRESSION_CODEC_FILE = "default_compression_codec.txt"
SERIALIZATION_FILE = "serialization.json"
BUILTIN_COMPRESSION_CODEC = b"CODEC(LZ4)"

# Synthetic names used while materializing recovered data.
RECOVERY_ROW_EXISTS_COLUMN = "_recovery_row_exists"
STAGING_PART_NAME = "all_0_0_0"

# Compatibility and part-layout constraints.
MIN_CLICKHOUSE_VERSION = "25.8"
MARK_SUFFIX_RE = re.compile(r"\.(?:c?mrk(?:[234])?)$")
MARK_FILE_RE = re.compile(r"\.(?:c?mrk)(?P<version>[^.]*)$")
WIDE_MARK_VERSIONS = {"", "2"}
COMPACT_MARK_VERSIONS = {"3", "4"}


@dataclass(frozen=True)
class _RecoveryPlan:
    """Prepared recovery state shared by the staging and target phases.

    Attributes:
        target: Destination table, explicit or generated.
        source: Resolved detached part and its owning table.
        disk_client: Client used to read and copy logical part files.
        file_recoverability: Whether every S3 object for each logical file exists.
        analysis: Columns, streams, and physical rows that can be recovered.
    """

    target: TableRef
    source: RecoverySource
    disk_client: ClickHouseDiskClient
    file_recoverability: Dict[str, bool]
    analysis: RecoveryAnalysis


def recover_part(
    ctx: Context,
    database: Optional[str],
    table: Optional[str],
    part_name: Optional[str],
    part_path: Optional[str],
    target_table: Optional[str],
    dry_run: bool = False,
) -> List[Dict[str, Any]]:
    """Recover intact source columns into a new persistent MergeTree table."""
    plan = _prepare_recovery(
        ctx,
        database,
        table,
        part_name,
        part_path,
        target_table,
        dry_run=dry_run,
    )
    if dry_run:
        return _make_report(plan.source, plan.target, plan.analysis, dry_run=True)

    with _staged_recovery_part(ctx, plan) as stage:
        _materialize_recovery_target(ctx, stage, plan)
    return _make_report(plan.source, plan.target, plan.analysis)


def _prepare_recovery(
    ctx: Context,
    database: Optional[str],
    table: Optional[str],
    part_name: Optional[str],
    part_path: Optional[str],
    target_table: Optional[str],
    dry_run: bool = False,
) -> _RecoveryPlan:
    """Resolve inputs, inspect S3 files, and analyze recoverable columns."""
    ch_version = get_version(ctx)
    if not version_ge(ch_version, MIN_CLICKHOUSE_VERSION):
        raise RuntimeError(
            f"Part recovery requires ClickHouse version {MIN_CLICKHOUSE_VERSION} or above"
        )

    explicit_target = (
        parse_qualified_table(target_table) if target_table is not None else None
    )
    source = resolve_recovery_source(ctx, database, table, part_name, part_path)
    target = explicit_target or _generated_target_table(source)
    _assert_target_absent(ctx, target)

    disk_conf = _get_s3_disk_configuration(ctx, source.disk.name)
    s3_client = boto3.client(
        "s3",
        endpoint_url=disk_conf.endpoint_url,
        aws_access_key_id=disk_conf.access_key_id,
        aws_secret_access_key=disk_conf.secret_access_key,
        config=Config(
            s3={"addressing_style": "auto"},
            retries=ctx.obj["config"]["object_storage"].get("retries"),
        ),
    )
    disk_client = ClickHouseDiskClient(source.disk.name)
    restored_files = restore_missing_empty_objects(
        source,
        s3_client,
        disk_conf,
        dry_run=dry_run,
    )
    file_recoverability = inspect_file_recoverability(source, s3_client, disk_conf)
    if dry_run:
        for filename in restored_files:
            file_recoverability[filename] = True
    analysis = _analyze_part(ctx, disk_client, source, file_recoverability)
    return _RecoveryPlan(
        target,
        source,
        disk_client,
        file_recoverability,
        analysis,
    )


def _generated_target_table(source: RecoverySource) -> TableRef:
    """Build the deterministic default target table reference."""
    return TableRef(
        source.table.ref.database,
        f"{source.table.ref.table}_recovered_{source.part_name}",
    )


def _generated_table(database: str, prefix: str) -> TableRef:
    """Build a collision-resistant internal table reference."""
    return TableRef(database, f"{prefix}{uuid.uuid4().hex}")


def _assert_target_absent(ctx: Context, target: TableRef) -> None:
    if table_exists(ctx, target.database, target.table):
        raise ValueError(f"Target table already exists: {target.display}")


def _get_s3_disk_configuration(ctx: Context, disk_name: str) -> S3DiskConfiguration:
    ch_config = get_clickhouse_config(ctx)
    disk_config = ch_config.storage_configuration.get_disk_config(disk_name)
    disk_type = disk_config.get("object_storage_type", disk_config.get("type"))
    if disk_type != S3DiskConfiguration.OBJECT_STORAGE_TYPE:
        raise ValueError(
            f"Part recovery supports only S3 disks; disk {disk_name} has type {disk_type}"
        )
    return S3DiskConfiguration.from_config(
        ch_config.storage_configuration,
        disk_name,
        ctx.obj["config"]["object_storage"]["bucket_name_prefix"],
    )


def _analyze_part(
    ctx: Context,
    disk_client: ClickHouseDiskClient,
    source: RecoverySource,
    file_recoverability: Dict[str, bool],
) -> RecoveryAnalysis:
    """Read structural metadata and select recoverable columns and streams."""
    # columns.txt and count.txt define the minimum viable staging part.
    _require_recoverable_file(file_recoverability, COLUMNS_FILE)
    _require_recoverable_file(file_recoverability, COUNT_FILE)
    columns_text = _read_source_file(disk_client, source, COLUMNS_FILE).decode()
    columns = parse_columns_text(columns_text)
    rows = int(_read_source_file(disk_client, source, COUNT_FILE).decode().strip())

    # Modern parts describe exact stream ownership and serialization.
    substreams = None
    if COLUMNS_SUBSTREAMS_FILE in file_recoverability:
        _require_recoverable_file(file_recoverability, COLUMNS_SUBSTREAMS_FILE)
        substreams = parse_columns_substreams(
            _read_source_file(disk_client, source, COLUMNS_SUBSTREAMS_FILE).decode()
        )
        validate_columns_substreams(columns, substreams)

    serialization = None
    if SERIALIZATION_FILE in file_recoverability:
        _require_recoverable_file(file_recoverability, SERIALIZATION_FILE)
        serialization = json.loads(
            _read_source_file(disk_client, source, SERIALIZATION_FILE)
        )

    # Mark-file versions encode the part layout. A Wide column named ``data``
    # also owns data.bin/data.mrk*, so filenames alone are not sufficient.
    if _detect_part_type(file_recoverability) == "Compact":
        analysis = _analyze_compact(
            columns, file_recoverability, rows, substreams, serialization
        )
    else:
        analysis = _analyze_wide(
            ctx, columns, file_recoverability, rows, substreams, serialization
        )

    if not analysis.recovered_user_columns:
        raise ValueError("No fully recoverable user columns found")
    if any(column.name == ROW_EXISTS_COLUMN for column in columns):
        if not analysis.has_row_exists:
            raise ValueError("The lightweight-delete mask is not fully recoverable")
    return analysis


def _detect_part_type(file_recoverability: Dict[str, bool]) -> str:
    """Determine the MergeTree part layout from ClickHouse mark versions."""
    part_types: Set[str] = set()
    unsupported: List[str] = []
    for filename in file_recoverability:
        match = MARK_FILE_RE.search(filename)
        if match is None:
            continue
        version = match.group("version")
        if version in WIDE_MARK_VERSIONS:
            part_types.add("Wide")
        elif version in COMPACT_MARK_VERSIONS:
            part_types.add("Compact")
        else:
            unsupported.append(filename)

    if unsupported:
        raise ValueError(
            "Unsupported ClickHouse mark file extensions: "
            + ", ".join(sorted(unsupported))
        )
    if not part_types:
        raise ValueError("Cannot determine part type: no ClickHouse mark files found")
    if len(part_types) != 1:
        raise ValueError("Conflicting ClickHouse mark versions in detached part")
    return next(iter(part_types))


def _require_recoverable_file(
    file_recoverability: Dict[str, bool], filename: str
) -> None:
    """Require a logical part file and all its S3 blobs to be readable."""
    if filename in file_recoverability and file_recoverability[filename]:
        return
    if filename == COLUMNS_FILE:
        raise ValueError(
            "Required structural file is not recoverable: columns.txt; "
            "the original part schema cannot be inferred safely after detach"
        )
    if filename == COUNT_FILE:
        raise ValueError(
            "Required structural file is not recoverable: count.txt; "
            "the physical row count is unavailable after detach"
        )
    raise ValueError(f"Required structural file is not recoverable: {filename}")


def _read_source_file(
    disk_client: ClickHouseDiskClient, source: RecoverySource, filename: str
) -> bytes:
    return disk_client.read(os.path.join(source.relative_path, filename))


def _analyze_compact(
    columns: List[PartColumn],
    file_recoverability: Dict[str, bool],
    rows: int,
    substreams: Optional[Dict[str, List[str]]],
    serialization: Optional[Dict[str, Any]],
) -> RecoveryAnalysis:
    """Analyze an all-or-nothing Compact part stored in shared data streams."""
    mark_files = sorted(
        name
        for name in file_recoverability
        if name.startswith("data.mrk") or name.startswith("data.cmrk")
    )
    required = {"data.bin", *mark_files}
    missing = sorted(
        name for name in required if not file_recoverability.get(name, False)
    )
    recovered = columns if not missing and mark_files else []
    lost = (
        {}
        if recovered
        else {column.name: missing or ["data marks"] for column in columns}
    )
    copy_files = set(required) if recovered else set()
    copy_files.add(COUNT_FILE)
    return RecoveryAnalysis(
        "Compact",
        columns,
        list(recovered),
        lost,
        copy_files,
        rows,
        substreams,
        serialization,
    )


def _analyze_wide(
    ctx: Context,
    columns: List[PartColumn],
    file_recoverability: Dict[str, bool],
    rows: int,
    substreams: Optional[Dict[str, List[str]]],
    serialization: Optional[Dict[str, Any]],
) -> RecoveryAnalysis:
    if substreams is not None:
        return _analyze_wide_with_substreams(
            ctx, columns, file_recoverability, rows, substreams, serialization
        )
    return _analyze_legacy_wide(columns, file_recoverability, rows, serialization)


def _analyze_wide_with_substreams(
    ctx: Context,
    columns: List[PartColumn],
    file_recoverability: Dict[str, bool],
    rows: int,
    substreams: Dict[str, List[str]],
    serialization: Optional[Dict[str, Any]],
) -> RecoveryAnalysis:
    """Recover Wide columns whose declared data and mark streams are complete."""
    # ClickHouse can store stream files under either readable or hashed names.
    all_streams = [
        stream for column in columns for stream in substreams.get(column.name, [])
    ]
    missing_stream_columns = [
        column.name for column in columns if not substreams.get(column.name)
    ]
    if missing_stream_columns:
        raise ValueError(
            "No stream metadata for columns: " + ", ".join(missing_stream_columns)
        )
    hashes = _hash_stream_names(ctx, all_streams)
    hash_by_stream = dict(zip(all_streams, hashes))

    recovered: List[PartColumn] = []
    lost: Dict[str, List[str]] = {}
    copy_files: Set[str] = {COUNT_FILE}
    for column in columns:
        required: Set[str] = set()
        missing: List[str] = []
        for stream in substreams[column.name]:
            base = _resolve_stream_base(
                stream, hash_by_stream[stream], file_recoverability
            )
            if base is None:
                missing.append(stream)
                continue
            data_file = base + ".bin"
            mark_files = _marks_for_base(base, file_recoverability)
            if (
                data_file not in file_recoverability
                or not file_recoverability[data_file]
            ):
                missing.append(data_file)
            if not mark_files:
                missing.append(base + ".mrk*")
            else:
                missing.extend(
                    mark for mark in mark_files if not file_recoverability[mark]
                )
            required.add(data_file)
            required.update(mark_files)
        if missing:
            lost[column.name] = sorted(set(missing))
        else:
            recovered.append(column)
            copy_files.update(required)

    return RecoveryAnalysis(
        "Wide",
        columns,
        recovered,
        lost,
        copy_files,
        rows,
        substreams,
        serialization,
    )


def _hash_stream_names(ctx: Context, streams: List[str]) -> List[str]:
    if not streams:
        return []
    values = ", ".join(quote_string(stream) for stream in streams)
    rows = execute_query(
        ctx,
        f"""
        SELECT arrayMap(
            x -> lower(hex(reverse(CAST(sipHash128(x), 'FixedString(16)')))),
            [{values}]
        )
        """,
        format_="JSONCompact",
    )["data"]
    return list(rows[0][0])


def _resolve_stream_base(
    stream: str, stream_hash: str, file_recoverability: Dict[str, bool]
) -> Optional[str]:
    if stream + ".bin" in file_recoverability or _marks_for_base(
        stream, file_recoverability
    ):
        return stream
    if stream_hash + ".bin" in file_recoverability or _marks_for_base(
        stream_hash, file_recoverability
    ):
        return stream_hash
    return None


def _marks_for_base(base: str, file_recoverability: Dict[str, bool]) -> List[str]:
    return sorted(
        name
        for name in file_recoverability
        if name.startswith(base + ".") and MARK_SUFFIX_RE.search(name)
    )


def _analyze_legacy_wide(
    columns: List[PartColumn],
    file_recoverability: Dict[str, bool],
    rows: int,
    serialization: Optional[Dict[str, Any]],
) -> RecoveryAnalysis:
    """Conservatively infer stream ownership for legacy Wide metadata."""
    data_files = [name for name in file_recoverability if name.endswith(".bin")]
    bases_by_column: Dict[str, Set[str]] = {}
    owner_groups_by_base: Dict[str, Set[str]] = {}

    # Legacy parts have no explicit column-to-stream map. Accept only
    # unambiguous filename prefixes, including Nested column owner groups.
    for column in columns:
        escaped = escape_for_file_name(column.name)
        owner_group = column.name.split(".", 1)[0]
        nested = escape_for_file_name(owner_group)
        bases = {
            name[:-4]
            for name in data_files
            if name[:-4] == escaped
            or name[:-4].startswith(escaped + ".")
            or name[:-4].startswith(escaped + "%2E")
            or (
                "." in column.name
                and (name[:-4] == nested or name[:-4].startswith(nested + "."))
            )
        }
        if not bases:
            raise ValueError(
                f"Cannot unambiguously map legacy streams for column {column.name}"
            )
        bases_by_column[column.name] = bases
        for base in bases:
            owner_groups_by_base.setdefault(base, set()).add(owner_group)

    ambiguous = sorted(
        base for base, owners in owner_groups_by_base.items() if len(owners) > 1
    )
    if ambiguous:
        raise ValueError(
            "Legacy streams match unrelated columns: " + ", ".join(ambiguous)
        )

    recovered: List[PartColumn] = []
    lost: Dict[str, List[str]] = {}
    copy_files: Set[str] = {COUNT_FILE}
    for column in columns:
        bases = bases_by_column[column.name]
        required = {base + ".bin" for base in bases}
        for base in bases:
            required.update(_marks_for_base(base, file_recoverability))
        missing = sorted(
            name
            for name in required
            if name not in file_recoverability
            or not file_recoverability.get(name, False)
        )
        if any(not _marks_for_base(base, file_recoverability) for base in bases):
            missing.append("marks")
        if missing:
            lost[column.name] = sorted(set(missing))
        else:
            recovered.append(column)
            copy_files.update(required)

    return RecoveryAnalysis(
        "Wide",
        columns,
        recovered,
        lost,
        copy_files,
        rows,
        None,
        serialization,
    )


@contextmanager
def _staged_recovery_part(
    ctx: Context, plan: _RecoveryPlan
) -> Generator[TableRef, None, None]:
    """Build and attach a readable part in a temporary staging table.

    The staging table lets ClickHouse validate the rebuilt part and read its
    intact columns. Recovered rows are copied from it into the separate,
    persistent target table.
    """
    stage = _generated_table(
        plan.source.table.ref.database,
        "_chadmin_recover_",
    )
    stage_columns = [
        column for column in plan.analysis.columns if column.name != ROW_EXISTS_COLUMN
    ]
    with _temporary_staging_table(
        ctx,
        stage,
        stage_columns,
        plan.source.table.storage_policy,
    ):
        stage_part_path = _prepare_staging_part(
            ctx,
            plan.disk_client,
            plan.source,
            stage,
            plan.analysis,
            plan.file_recoverability,
        )
        logging.info("Prepared recovery staging part at {}", stage_part_path)
        attach_part(
            ctx,
            stage.database,
            stage.table,
            STAGING_PART_NAME,
            echo=False,
        )
        active_part = _get_only_active_part(ctx, stage)
        if not check_table(ctx, stage.database, stage.table, part=active_part):
            raise RuntimeError("CHECK TABLE failed for the staging part")

        mask_settings = (
            " SETTINGS apply_deleted_mask = 0" if plan.analysis.has_row_exists else ""
        )
        physical_rows = int(
            execute_query(
                ctx,
                f"SELECT count() FROM {stage.sql}{mask_settings}",
                format_=None,
            )
        )
        if physical_rows != plan.analysis.rows:
            raise RuntimeError(
                "Staging row count differs: "
                f"expected {plan.analysis.rows}, got {physical_rows}"
            )
        yield stage


@contextmanager
def _temporary_staging_table(
    ctx: Context,
    table: TableRef,
    columns: List[PartColumn],
    storage_policy: str,
) -> Generator[None, None, None]:
    """Create an internal staging table and remove it on every exit.

    This table is only scratch space for attaching and reading the rebuilt
    part; it is never the recovery result. Its collision-resistant name is
    generated by chadmin, so this context owns its complete lifecycle.
    """
    failure: Optional[BaseException] = None
    try:
        _create_recovery_table(ctx, table, columns, storage_policy)
        yield
    except BaseException as e:
        failure = e
        raise
    finally:
        # Cleanup is attempted even if CREATE TABLE or subsequent staging work
        # failed, so neither ClickHouse metadata nor staging data is left behind.
        try:
            delete_table(ctx, table.database, table.table)
        except Exception:
            if failure is None:
                raise
            # Keep the original recovery error as the primary failure.
            logging.exception(
                "Failed to clean up temporary staging table {}", table.display
            )


def _create_recovery_table(
    ctx: Context,
    table: TableRef,
    columns: List[PartColumn],
    storage_policy: str,
) -> None:
    if not columns:
        raise ValueError("Cannot create a recovery table without columns")
    definitions = ",\n".join(column.definition for column in columns)
    policy = (
        f" SETTINGS storage_policy = {quote_string(storage_policy)}"
        if storage_policy
        else ""
    )
    execute_query(
        ctx,
        f"""
        CREATE TABLE {table.sql}
        (
            {definitions}
        )
        ENGINE = MergeTree
        ORDER BY tuple()
        {policy}
        """,
        format_=None,
    )


def _prepare_staging_part(
    ctx: Context,
    disk_client: ClickHouseDiskClient,
    source: RecoverySource,
    stage: TableRef,
    analysis: RecoveryAnalysis,
    file_recoverability: Dict[str, bool],
) -> str:
    """Copy intact streams and regenerate metadata for the staging part.

    Since 26.9 ClickHouse refuses to attach a part without
    ``default_compression_codec.txt`` instead of deducing the codec, so the file
    is carried over from the source part, or written with the built-in default
    when the source copy is lost.
    """
    stage_root = _get_table_path_on_disk(ctx, stage, source.disk)
    relative_stage_root = str(stage_root.relative_to(source.disk.root))
    detached_root = os.path.join(relative_stage_root, "detached")
    stage_part = os.path.join(detached_root, STAGING_PART_NAME)
    disk_client.mkdir(stage_part, parents=True)

    for filename in sorted(analysis.files_to_copy):
        _require_recoverable_file(file_recoverability, filename)
        disk_client.copy(
            os.path.join(source.relative_path, filename),
            os.path.join(stage_part, filename),
        )

    codec_path = os.path.join(stage_part, DEFAULT_COMPRESSION_CODEC_FILE)
    if file_recoverability.get(DEFAULT_COMPRESSION_CODEC_FILE):
        disk_client.copy(
            os.path.join(source.relative_path, DEFAULT_COMPRESSION_CODEC_FILE),
            codec_path,
        )
    else:
        disk_client.write(codec_path, BUILTIN_COMPRESSION_CODEC)

    disk_client.write(
        os.path.join(stage_part, COLUMNS_FILE),
        render_columns_text(analysis.recovered_columns),
    )
    if analysis.serialization is not None:
        disk_client.write(
            os.path.join(stage_part, SERIALIZATION_FILE),
            filter_serialization(analysis.serialization, analysis.recovered_columns),
        )
    if analysis.columns_substreams is not None:
        disk_client.write(
            os.path.join(stage_part, COLUMNS_SUBSTREAMS_FILE),
            render_columns_substreams(
                analysis.columns_substreams, analysis.recovered_columns
            ),
        )
    return stage_part


def _get_table_path_on_disk(ctx: Context, table: TableRef, disk: DiskInfo) -> Path:
    rows = execute_query(
        ctx,
        f"""
        SELECT data_paths
        FROM system.tables
        WHERE database = {quote_string(table.database)}
          AND name = {quote_string(table.table)}
        """,
        format_="JSONCompact",
    )["data"]
    if len(rows) != 1:
        raise RuntimeError(f"Cannot get data paths for {table.display}")
    paths = [
        Path(item).resolve()
        for item in rows[0][0]
        if Path(item).resolve().is_relative_to(disk.root)
    ]
    if len(paths) != 1:
        raise RuntimeError(
            f"Cannot find a unique path for {table.display} on disk {disk.name}"
        )
    return paths[0]


def _get_only_active_part(ctx: Context, table: TableRef) -> str:
    rows = [
        row
        for row in list_parts(
            ctx,
            database=table.database,
            table=table.table,
            active=True,
        )
        if row["database"] == table.database and row["table"] == table.table
    ]
    if len(rows) != 1:
        raise RuntimeError(
            f"Expected one active staging part for {table.display}, found {len(rows)}"
        )
    return str(rows[0]["name"])


def _materialize_recovery_target(
    ctx: Context, stage: TableRef, plan: _RecoveryPlan
) -> None:
    """Create and validate the persistent target, removing it after failures."""
    target_created = False
    try:
        execute_query(
            ctx,
            f"CREATE DATABASE IF NOT EXISTS "
            f"{quote_identifier(plan.target.database)}",
            format_=None,
        )
        target_columns = list(plan.analysis.recovered_user_columns)
        if plan.analysis.has_row_exists:
            target_columns.append(
                PartColumn(
                    RECOVERY_ROW_EXISTS_COLUMN,
                    "UInt8",
                    f"{quote_identifier(RECOVERY_ROW_EXISTS_COLUMN)} UInt8",
                )
            )
        _create_recovery_table(
            ctx,
            plan.target,
            target_columns,
            plan.source.table.storage_policy,
        )
        target_created = True
        _insert_recovered_data(ctx, stage, plan.target, plan.analysis)

        target_rows = int(
            execute_query(
                ctx,
                f"SELECT count() FROM {plan.target.sql}",
                format_=None,
            )
        )
        if target_rows != plan.analysis.rows:
            raise RuntimeError(
                "Target row count differs: "
                f"expected {plan.analysis.rows}, got {target_rows}"
            )
        if not check_table(ctx, plan.target.database, plan.target.table):
            raise RuntimeError("CHECK TABLE failed for the recovery target")
    except BaseException:
        if target_created:
            try:
                delete_table(ctx, plan.target.database, plan.target.table)
            except Exception:
                logging.exception(
                    "Failed to clean up incomplete recovery target {}",
                    plan.target.display,
                )
        raise


def _insert_recovered_data(
    ctx: Context,
    stage: TableRef,
    target: TableRef,
    analysis: RecoveryAnalysis,
) -> None:
    target_names = [
        quote_identifier(column.name) for column in analysis.recovered_user_columns
    ]
    select_names = list(target_names)
    if analysis.has_row_exists:
        target_names.append(quote_identifier(RECOVERY_ROW_EXISTS_COLUMN))
        select_names.append(
            f"{quote_identifier(ROW_EXISTS_COLUMN)} AS "
            f"{quote_identifier(RECOVERY_ROW_EXISTS_COLUMN)}"
        )
    mask_settings = (
        " SETTINGS apply_deleted_mask = 0" if analysis.has_row_exists else ""
    )
    execute_query(
        ctx,
        f"""
        INSERT INTO {target.sql} ({", ".join(target_names)})
        SELECT {", ".join(select_names)}
        FROM {stage.sql}
        {mask_settings}
        """,
        format_=None,
    )


def _make_report(
    source: RecoverySource,
    target: TableRef,
    analysis: RecoveryAnalysis,
    dry_run: bool = False,
) -> List[Dict[str, Any]]:
    recovered_names = {column.name for column in analysis.recovered_columns}
    result: List[Dict[str, Any]] = []
    for column in analysis.columns:
        recovered = column.name in recovered_names
        target_column: Optional[str] = column.name if recovered else None
        if column.name == ROW_EXISTS_COLUMN and recovered:
            target_column = RECOVERY_ROW_EXISTS_COLUMN
        result.append(
            {
                "source_table": source.table.ref.display,
                "source_part": source.part_name,
                "source_path": str(source.path),
                "target_table": target.display,
                "part_type": analysis.part_type,
                "source_column": column.name,
                "target_column": target_column,
                "type": column.type,
                "status": (
                    ("recoverable" if dry_run else "recovered") if recovered else "lost"
                ),
                "missing_files": analysis.lost_files_by_column.get(column.name, []),
                "physical_rows": analysis.rows,
            }
        )
    return result
