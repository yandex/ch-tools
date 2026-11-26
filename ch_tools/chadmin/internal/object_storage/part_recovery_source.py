"""Resolve detached ClickHouse parts and inspect their S3 file availability."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from boto3 import client as Boto3Client
from click import Context

from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    S3ObjectLocalMetaData,
    get_object_storage_key,
    object_exists,
    restore_empty_object,
)
from ch_tools.chadmin.internal.part import get_disks, list_detached_parts
from ch_tools.chadmin.internal.table import list_tables
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.clickhouse.client.sql import TableRef
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration


@dataclass(frozen=True)
class SourceTable:
    """MergeTree table properties needed to locate and stage its data."""

    ref: TableRef
    storage_policy: str
    data_paths: List[str]


@dataclass(frozen=True)
class DiskInfo:
    """Configured ClickHouse disk name and its resolved local root."""

    name: str
    root: Path


@dataclass(frozen=True)
class RecoverySource:
    """Resolved location and ownership of the detached source part.

    Attributes:
        table: MergeTree table that owns the part.
        disk: ClickHouse disk containing the local part metadata.
        path: Absolute local path to the detached part.
        relative_path: Path accepted by clickhouse-disks.
        part_name: Part name displayed in the recovery report.
    """

    table: SourceTable
    disk: DiskInfo
    path: Path
    relative_path: str
    part_name: str


def parse_qualified_table(value: str) -> TableRef:
    """Parse a required DATABASE.TABLE target reference."""
    if "." not in value:
        raise ValueError("Target table must be specified as DATABASE.TABLE")
    database, table = value.split(".", 1)
    if not database or not table:
        raise ValueError("Target table must be specified as DATABASE.TABLE")
    return TableRef(database, table)


def resolve_recovery_source(
    ctx: Context,
    database: Optional[str],
    table: Optional[str],
    part_name: Optional[str],
    part_path: Optional[str],
) -> RecoverySource:
    """Resolve the source table, disk, and part path from CLI selectors.

    A detached-part system row is authoritative. For paths moved outside the
    standard detached directory, an explicit source table acts as a fallback.
    """
    if bool(part_name) == bool(part_path):
        raise ValueError("Specify exactly one of --name and --path")
    if bool(database) != bool(table):
        raise ValueError("--database and --table must be specified together")
    if part_name and not database:
        raise ValueError("--name requires --database and --table")
    if part_path is not None and not Path(part_path).is_absolute():
        raise ValueError("--path must be absolute")

    tables = _list_merge_tree_tables(ctx)
    source_ref = TableRef(database, table) if database and table else None

    if part_name:
        assert source_ref is not None
        detached = _find_detached_part(ctx, source_ref, part_name)
        path = Path(detached["path"]).resolve()
        display_part_name = detached["name"]
    else:
        assert part_path is not None
        path = Path(part_path).resolve()
        inferred_ref = _infer_table_from_path(ctx, path, tables)
        display_part_name = path.name
        if source_ref and inferred_ref and source_ref != inferred_ref:
            raise ValueError(
                f"Path belongs to {inferred_ref.display}, not {source_ref.display}"
            )
        source_ref = source_ref or inferred_ref

    if source_ref is None:
        raise ValueError(
            "Cannot infer source table from --path; specify --database and --table"
        )

    source_table = next((item for item in tables if item.ref == source_ref), None)
    if source_table is None:
        raise ValueError(f"Source table {source_ref.display} does not exist")
    if not path.is_dir():
        raise ValueError(f"Part path is not a directory: {path}")

    disk = _find_disk_for_path(ctx, path)
    _validate_policy_disk(ctx, source_table.storage_policy, disk.name)
    relative_path = str(path.relative_to(disk.root))
    return RecoverySource(
        source_table,
        disk,
        path,
        relative_path,
        display_part_name,
    )


def _list_merge_tree_tables(ctx: Context) -> List[SourceTable]:
    return [
        SourceTable(
            TableRef(row["database"], row["name"]),
            row["storage_policy"],
            list(row["data_paths"]),
        )
        for row in list_tables(ctx, engine_pattern="%MergeTree%")
    ]


def _find_detached_part(
    ctx: Context, table: TableRef, part_name: str
) -> Dict[str, Any]:
    rows = [
        row
        for row in list_detached_parts(
            ctx,
            exact_table=True,
            exact_part=True,
            database=table.database,
            table=table.table,
            part_name=part_name,
        )
        if row["database"] == table.database
        and row["table"] == table.table
        and row["name"] == part_name
    ]
    if len(rows) != 1:
        raise ValueError(
            f"Expected one detached part {table.display}.{part_name}, found {len(rows)}"
        )
    return rows[0]


def _infer_table_from_path(
    ctx: Context, path: Path, tables: List[SourceTable]
) -> Optional[TableRef]:
    # Prefer ClickHouse's authoritative detached-parts catalog.
    normalized = str(path).rstrip("/") + "/"
    rows = execute_query(
        ctx,
        """
        SELECT database, table
        FROM system.detached_parts
        WHERE path IN {paths:Array(String)}
        """,
        parameters={"paths": [normalized, str(path)]},
        format_="JSON",
    )["data"]
    refs = {TableRef(row["database"], row["table"]) for row in rows}
    if len(refs) > 1:
        raise ValueError(f"Part path matches multiple tables: {path}")
    if refs:
        return next(iter(refs))

    # A manually moved part is absent from system.detached_parts. In that
    # case, infer ownership only when it remains below one table's data path.
    matches = []
    for source_table in tables:
        for data_path in source_table.data_paths:
            detached_root = Path(data_path).resolve() / "detached"
            if path.is_relative_to(detached_root):
                matches.append(source_table.ref)
                break
    unique_matches = set(matches)
    if len(unique_matches) > 1:
        raise ValueError(f"Part path matches multiple table data paths: {path}")
    return next(iter(unique_matches)) if unique_matches else None


def _find_disk_for_path(ctx: Context, path: Path) -> DiskInfo:
    disks = [
        DiskInfo(name, Path(info["path"]).resolve())
        for name, info in get_disks(ctx).items()
        if path.is_relative_to(Path(info["path"]).resolve())
    ]
    if not disks:
        raise ValueError(f"Part path is outside configured ClickHouse disks: {path}")
    return max(disks, key=lambda disk: len(str(disk.root)))


def _validate_policy_disk(ctx: Context, policy: str, disk: str) -> None:
    rows = execute_query(
        ctx,
        """
        SELECT 1
        FROM system.storage_policies
        WHERE policy_name = {policy:String}
          AND has(disks, {disk:String})
        LIMIT 1
        """,
        parameters={"policy": policy, "disk": disk},
        format_="JSONCompact",
    )["data"]
    if not rows:
        raise ValueError(f"Disk {disk} is not part of storage policy {policy}")


def inspect_file_recoverability(
    source: RecoverySource,
    s3_client: Boto3Client,
    disk_conf: S3DiskConfiguration,
) -> Dict[str, bool]:
    """Map logical part files to the availability of all referenced S3 blobs."""
    result: Dict[str, bool] = {}
    for path in sorted(source.path.iterdir()):
        if not path.is_file():
            continue
        try:
            metadata = S3ObjectLocalMetaData.from_file(path)
            result[path.name] = all(
                object_exists(
                    s3_client,
                    disk_conf.bucket_name,
                    get_object_storage_key(disk_conf.prefix, object_info),
                )
                for object_info in metadata.objects
            )
        except (OSError, ValueError) as e:
            logging.warning("Cannot parse metadata file {}: {!r}", path, e)
            result[path.name] = False
    return result


def restore_missing_empty_objects(
    source: RecoverySource,
    s3_client: Boto3Client,
    disk_conf: S3DiskConfiguration,
    dry_run: bool = False,
) -> List[str]:
    """Restore or simulate logical files with known-empty missing S3 objects."""
    restored_files: List[str] = []
    for path in sorted(source.path.iterdir()):
        if not path.is_file():
            continue
        try:
            metadata = S3ObjectLocalMetaData.from_file(path)
            missing_objects = [
                (
                    object_info,
                    get_object_storage_key(disk_conf.prefix, object_info),
                )
                for object_info in metadata.objects
                if not object_exists(
                    s3_client,
                    disk_conf.bucket_name,
                    get_object_storage_key(disk_conf.prefix, object_info),
                )
            ]
        except (OSError, ValueError) as e:
            logging.warning("Cannot parse metadata file {}: {!r}", path, e)
            continue

        if not missing_objects or any(
            object_info.size != 0 for object_info, _ in missing_objects
        ):
            continue

        if not dry_run:
            for _, object_key in missing_objects:
                if not restore_empty_object(
                    s3_client, disk_conf.bucket_name, object_key
                ):
                    raise RuntimeError(
                        f"Empty object restoration verification failed for {path.name}"
                    )
        restored_files.append(path.name)
        logging.info(
            "{} {} missing empty S3 object(s) for {}",
            "Would restore" if dry_run else "Restored",
            len(missing_objects),
            path,
        )
    return restored_files
