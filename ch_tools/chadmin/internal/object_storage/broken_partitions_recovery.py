from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from boto3 import client as Boto3Client
from click import Context

from ch_tools.chadmin.internal.object_storage.s3_object_checker import (
    ObjectCheckResult,
    ObjectCheckStatus,
    S3ObjectChecker,
)
from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    S3ObjectLocalInfo,
    S3ObjectLocalMetaData,
    get_object_storage_key,
    restore_empty_object,
)
from ch_tools.chadmin.internal.part import (
    list_detached_parts,
    list_parts,
)
from ch_tools.chadmin.internal.partition import (
    attach_partition as attach_clickhouse_partition,
)
from ch_tools.chadmin.internal.partition import (
    detach_partition as detach_clickhouse_partition,
)
from ch_tools.common import logging
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration

ATTACH_DETACH_QUERY_RETRY = 10
OBJECT_CHECK_MODES = ("head", "range", "full")


@dataclass(frozen=True, order=True)
class PartitionKey:
    database: str
    table: str
    partition_id: str


@dataclass(frozen=True)
class BrokenPartInfo:
    database: str
    table: str
    partition_id: str
    name: str
    part_type: str
    rows: int
    default_compression_codec: str

    @property
    def table_id(self) -> str:
        return f"`{self.database}`.`{self.table}`"


@dataclass(frozen=True)
class ObjectIssue:
    filename: str
    metadata_path: Path
    metadata: S3ObjectLocalMetaData
    object_info: S3ObjectLocalInfo
    check: ObjectCheckResult

    @property
    def object_key(self) -> str:
        return self.check.key


@dataclass
class BrokenPart:
    path: str
    info: BrokenPartInfo
    object_issues: List[ObjectIssue]
    objects_checked: int = 0

    @property
    def missing_objects(self) -> List[ObjectIssue]:
        return [
            issue
            for issue in self.object_issues
            if issue.check.status == ObjectCheckStatus.MISSING
        ]


@dataclass
class PartRecoveryResult:
    part_info: BrokenPartInfo
    reports: List[Dict[str, Any]]
    attach_files: List[str]
    has_unresolved: bool


def restore_recoverable_broken_partitions(
    ctx: Context,
    s3_client: Boto3Client,
    disk_conf: S3DiskConfiguration,
    broken_parts: List[BrokenPart],
    detach_unrecoverable: bool,
    reattach_unrecoverable: bool,
) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []

    broken_parts_by_partition = group_broken_parts_by_partition(broken_parts)

    for partition_key in sorted(broken_parts_by_partition):
        partition_broken_parts = broken_parts_by_partition[partition_key]
        recovery_results = [
            restore_recoverable_broken_part(
                broken_part=broken_part,
                s3_client=s3_client,
                bucket=disk_conf.bucket_name,
                prefix=disk_conf.prefix,
            )
            for broken_part in partition_broken_parts
        ]
        for recovery_result in recovery_results:
            result.extend(recovery_result.reports)

        attach_requests = [
            (recovery_result.part_info, recovery_result.attach_files)
            for recovery_result in recovery_results
            if recovery_result.attach_files
        ]
        excluded_parts = [
            recovery_result.part_info
            for recovery_result in recovery_results
            if recovery_result.has_unresolved and not recovery_result.attach_files
        ]
        partition_reattached: Optional[bool] = None
        attach_reports: List[Dict[str, Any]] = []
        if attach_requests:
            attach_reports, partition_reattached = restore_files_via_attach(
                ctx, attach_requests, excluded_parts
            )
            result.extend(attach_reports)

        has_unresolved = any(
            recovery_result.has_unresolved for recovery_result in recovery_results
        ) or reports_have_unresolved(attach_reports)
        if not has_unresolved:
            continue

        partition_info = partition_broken_parts[0].info
        if detach_unrecoverable:
            result.append(detach_unrecoverable_partition(ctx, partition_info))
        elif reattach_unrecoverable:
            if partition_reattached is None:
                result.append(reattach_unrecoverable_partition(ctx, partition_info))
            else:
                result.append(
                    make_partition_action_report(
                        partition_info,
                        reattach=True,
                        success=partition_reattached,
                    )
                )

    return result


def group_broken_parts_by_partition(
    broken_parts: List[BrokenPart],
) -> Dict[PartitionKey, List[BrokenPart]]:
    result: Dict[PartitionKey, List[BrokenPart]] = {}
    for broken_part in broken_parts:
        part_info = broken_part.info
        partition_key = PartitionKey(
            database=part_info.database,
            table=part_info.table,
            partition_id=part_info.partition_id,
        )
        result.setdefault(partition_key, []).append(broken_part)
    return result


def find_broken_parts(
    ctx: Context,
    root_path: str,
    s3_client: Boto3Client,
    disk_conf: S3DiskConfiguration,
    mode: str = "head",
) -> Tuple[List[BrokenPart], int]:
    if mode not in OBJECT_CHECK_MODES:
        raise ValueError(f"Unsupported S3 object check mode: {mode}")

    active_parts_by_path = get_active_parts_by_path(ctx)
    broken_parts_by_path: Dict[str, BrokenPart] = {}
    objects_checked_by_path: Dict[str, int] = {}
    checked_part_paths: Set[str] = set()
    checker = S3ObjectChecker(
        s3_client,
        disk_conf.bucket_name,
        disk_conf.prefix,
        mode,
    )

    for path, _, files in os.walk(root_path):
        part_info = active_parts_by_path.get(f"{path}/")
        if part_info is None:
            continue
        checked_part_paths.add(path)

        for file in files:
            metadata_path = Path(path) / file
            try:
                metadata = S3ObjectLocalMetaData.from_file(metadata_path)
            except Exception as e:
                logging.error(
                    "Failed to read object metadata {}: {!r}", metadata_path, e
                )
                continue

            for object_info in metadata.objects:
                objects_checked_by_path[path] = objects_checked_by_path.get(path, 0) + 1
                check = checker.inspect(object_info)
                if check.status == ObjectCheckStatus.OK:
                    continue

                broken_part = broken_parts_by_path.setdefault(
                    path,
                    BrokenPart(path=path, info=part_info, object_issues=[]),
                )
                broken_part.object_issues.append(
                    ObjectIssue(
                        filename=file,
                        metadata_path=metadata_path,
                        metadata=metadata,
                        object_info=object_info,
                        check=check,
                    )
                )

    for path, broken_part in broken_parts_by_path.items():
        broken_part.objects_checked = objects_checked_by_path[path]
    return (
        sorted(broken_parts_by_path.values(), key=lambda item: item.path),
        len(checked_part_paths),
    )


def restore_recoverable_broken_part(
    broken_part: BrokenPart,
    s3_client: Boto3Client,
    bucket: str,
    prefix: str,
) -> PartRecoveryResult:
    report: List[Dict[str, Any]] = []

    missing_by_file: Dict[str, List[ObjectIssue]] = {}
    for missing_object in broken_part.missing_objects:
        missing_by_file.setdefault(missing_object.filename, []).append(missing_object)

    for issue in broken_part.object_issues:
        if issue.check.status == ObjectCheckStatus.MISSING:
            continue
        report.append(
            make_restore_report(
                broken_part.info,
                issue.filename,
                "unrecoverable",
                f"object check is {issue.check.status.value}: {issue.check.reason}",
            )
        )

    supported_attach_files = {"checksums.txt", "columns.txt"}
    attach_files = sorted(set(missing_by_file).intersection(supported_attach_files))

    for filename, missing_objects in sorted(missing_by_file.items()):
        if filename in supported_attach_files:
            continue
        if all(item.object_info.size == 0 for item in missing_objects):
            report.extend(
                restore_empty_objects(
                    broken_part.info, missing_objects, s3_client, bucket
                )
            )
        elif filename == "count.txt":
            report.append(
                restore_generated_file(
                    broken_part.info,
                    filename,
                    str(broken_part.info.rows).encode("utf-8"),
                    missing_objects,
                    s3_client,
                    bucket,
                    prefix,
                )
            )
        elif filename == "default_compression_codec.txt":
            codec = broken_part.info.default_compression_codec
            if not codec.startswith("CODEC("):
                codec = f"CODEC({codec})"
            report.append(
                restore_generated_file(
                    broken_part.info,
                    filename,
                    codec.encode("utf-8"),
                    missing_objects,
                    s3_client,
                    bucket,
                    prefix,
                )
            )
        else:
            report.append(
                make_restore_report(
                    broken_part.info,
                    filename,
                    "unrecoverable",
                    "file is not safely recoverable in v1",
                )
            )

    if "columns.txt" in attach_files and not can_restore_columns_txt(broken_part.info):
        for filename in attach_files:
            report.append(
                make_restore_report(
                    broken_part.info,
                    filename,
                    "unrecoverable" if filename == "columns.txt" else "skipped",
                    (
                        "columns.txt can be restored only for Wide parts"
                        if filename == "columns.txt"
                        else "metadata restore via attach is skipped while columns.txt is unrecoverable"
                    ),
                )
            )
        attach_files = []
    elif reports_have_unresolved(report):
        for filename in attach_files:
            report.append(
                make_restore_report(
                    broken_part.info,
                    filename,
                    "skipped",
                    "metadata restore via attach is skipped while other files are missing",
                )
            )
        attach_files = []

    return PartRecoveryResult(
        part_info=broken_part.info,
        reports=report,
        attach_files=attach_files,
        has_unresolved=reports_have_unresolved(report),
    )


def restore_empty_objects(
    part_info: BrokenPartInfo,
    missing_objects: List[ObjectIssue],
    s3_client: Boto3Client,
    bucket: str,
) -> List[Dict[str, Any]]:
    report: List[Dict[str, Any]] = []
    for missing_object in missing_objects:
        if restore_empty_object(s3_client, bucket, missing_object.object_key):
            report.append(
                make_restore_report(
                    part_info,
                    missing_object.filename,
                    "restored",
                    "uploaded empty object",
                )
            )
        else:
            report.append(
                make_restore_report(
                    part_info,
                    missing_object.filename,
                    "unrecoverable",
                    "empty object upload verification failed",
                )
            )
    return report


def restore_generated_file(
    part_info: BrokenPartInfo,
    filename: str,
    content: bytes,
    missing_objects: List[ObjectIssue],
    s3_client: Boto3Client,
    bucket: str,
    prefix: str,
) -> Dict[str, Any]:
    metadata = missing_objects[0].metadata
    total_object_size = sum(item.size for item in metadata.objects)
    if metadata.total_size != len(content) or total_object_size != len(content):
        return make_restore_report(
            part_info,
            filename,
            "unrecoverable",
            f"generated size {len(content)} does not match metadata size {metadata.total_size}",
        )

    missing_keys = {item.object_key for item in missing_objects}
    offset = 0
    chunks_to_upload: List[Tuple[str, bytes]] = []
    for object_info in metadata.objects:
        object_key = get_object_storage_key(prefix, object_info)
        chunk = content[offset : offset + object_info.size]
        offset += object_info.size
        if object_key in missing_keys:
            chunks_to_upload.append((object_key, chunk))
            continue

        current = s3_client.get_object(Bucket=bucket, Key=object_key)["Body"].read()
        if current != chunk:
            return make_restore_report(
                part_info,
                filename,
                "unrecoverable",
                "existing object content does not match generated content",
            )

    for object_key, chunk in chunks_to_upload:
        s3_client.put_object(Bucket=bucket, Key=object_key, Body=chunk)

    return make_restore_report(
        part_info,
        filename,
        "restored",
        "uploaded generated content with exact metadata size",
    )


def restore_files_via_attach(
    ctx: Context,
    attach_requests: List[Tuple[BrokenPartInfo, List[str]]],
    excluded_parts: List[BrokenPartInfo],
) -> Tuple[List[Dict[str, Any]], bool]:
    partition_info = attach_requests[0][0]
    if not detach_partition_with_retry(ctx, partition_info):
        return (
            [
                make_restore_report(
                    part_info,
                    filename,
                    "detach_failed",
                    "failed to detach partition before metadata regeneration",
                )
                for part_info, filenames in attach_requests
                for filename in filenames
            ],
            False,
        )

    affected_parts = [part_info for part_info, _ in attach_requests] + excluded_parts
    detached_part_paths = {
        part_info.name: get_detached_part_path(ctx, part_info)
        for part_info in affected_parts
    }
    missing_part_names = sorted(
        part_info.name
        for part_info in affected_parts
        if detached_part_paths[part_info.name] is None
    )
    if missing_part_names:
        attach_succeeded = attach_partition_with_retry(ctx, partition_info)
        return (
            [
                make_restore_report(
                    part_info,
                    filename,
                    "unrecoverable",
                    "detached part paths were not found: "
                    + ", ".join(missing_part_names),
                )
                for part_info, filenames in attach_requests
                for filename in filenames
            ],
            attach_succeeded,
        )

    isolated_parts: List[Tuple[Path, Path]] = []
    try:
        isolated_parts = isolate_detached_parts(detached_part_paths, excluded_parts)
    except OSError as e:
        restore_isolated_part_paths(isolated_parts)
        attach_succeeded = attach_partition_with_retry(ctx, partition_info)
        return (
            [
                make_restore_report(
                    part_info,
                    filename,
                    "unrecoverable",
                    f"failed to isolate unrecoverable part: {e}",
                )
                for part_info, filenames in attach_requests
                for filename in filenames
            ],
            attach_succeeded,
        )

    backups_by_part: Dict[str, List[Tuple[Path, Path]]] = {}
    try:
        for part_info, filenames in attach_requests:
            detached_part_path = detached_part_paths[part_info.name]
            assert detached_part_path is not None
            for filename in filenames:
                pointer_path = detached_part_path / filename
                if pointer_path.exists():
                    backup_path = make_backup_path(
                        detached_part_path, part_info.name, filename
                    )
                    pointer_path.rename(backup_path)
                    backups_by_part.setdefault(part_info.name, []).append(
                        (backup_path, pointer_path)
                    )
    except OSError as e:
        for backups in backups_by_part.values():
            restore_backup_paths(backups)
        restore_isolated_part_paths(isolated_parts)
        attach_succeeded = attach_partition_with_retry(ctx, partition_info)
        return (
            [
                make_restore_report(
                    part_info,
                    filename,
                    "unrecoverable",
                    f"failed to move metadata pointer: {e}",
                )
                for part_info, filenames in attach_requests
                for filename in filenames
            ],
            attach_succeeded,
        )

    attach_succeeded = attach_partition_with_retry(ctx, partition_info)
    restore_isolated_part_paths(isolated_parts)
    if attach_succeeded:
        for backups in backups_by_part.values():
            for backup_path, _ in backups:
                cleanup_backup_path(backup_path)
        return (
            [
                make_restore_report(
                    part_info,
                    filename,
                    "restored",
                    "regenerated by ClickHouse ATTACH PARTITION",
                )
                for part_info, filenames in attach_requests
                for filename in filenames
            ],
            True,
        )

    active_part_names = {
        part["name"]
        for part in list_parts(
            ctx,
            database=partition_info.database,
            table=partition_info.table,
            partition_id=partition_info.partition_id,
            active=True,
        )
    }
    reports: List[Dict[str, Any]] = []
    all_requested_parts_attached = True
    for part_info, filenames in attach_requests:
        part_attached = part_info.name in active_part_names
        all_requested_parts_attached = all_requested_parts_attached and part_attached
        backups = backups_by_part.get(part_info.name, [])
        if part_attached:
            for backup_path, _ in backups:
                cleanup_backup_path(backup_path)
        else:
            restore_backup_paths(backups)
        for filename in filenames:
            reports.append(
                make_restore_report(
                    part_info,
                    filename,
                    "restored" if part_attached else "attach_failed",
                    (
                        "regenerated by ClickHouse ATTACH PARTITION"
                        if part_attached
                        else "ClickHouse failed to attach part after pointer removal"
                    ),
                )
            )
    return reports, all_requested_parts_attached


def detach_broken_partition(
    ctx: Context, part_info: BrokenPartInfo, reattach: bool = True
) -> bool:
    if not detach_partition_with_retry(ctx, part_info):
        return False
    if reattach:
        return attach_partition_with_retry(ctx, part_info)
    return True


def detach_unrecoverable_partition(
    ctx: Context, part_info: BrokenPartInfo
) -> Dict[str, Any]:
    success = detach_broken_partition(ctx, part_info, reattach=False)
    return make_partition_action_report(part_info, reattach=False, success=success)


def reattach_unrecoverable_partition(
    ctx: Context, part_info: BrokenPartInfo
) -> Dict[str, Any]:
    success = detach_broken_partition(ctx, part_info, reattach=True)
    return make_partition_action_report(part_info, reattach=True, success=success)


def make_partition_action_report(
    part_info: BrokenPartInfo, reattach: bool, success: bool
) -> Dict[str, Any]:
    if reattach:
        status = "reattached" if success else "reattach_failed"
        detail = (
            "healthy and restored parts were reattached; "
            "unrecoverable parts remain detached"
            if success
            else "failed to reattach partition after recoverable restore"
        )
    else:
        status = "detached" if success else "detach_failed"
        detail = (
            "partition has unrecoverable missing objects"
            if success
            else "failed to detach partition with unrecoverable missing objects"
        )
    return make_restore_report(part_info, "*", status, detail)


def reports_have_unresolved(reports: List[Dict[str, Any]]) -> bool:
    return any(report["status"] != "restored" for report in reports)


def can_restore_columns_txt(part_info: BrokenPartInfo) -> bool:
    return part_info.part_type.lower() == "wide"


def make_backup_path(detached_part_path: Path, part_name: str, filename: str) -> Path:
    descriptor, backup_path = tempfile.mkstemp(
        prefix=f".{part_name}.{filename}.",
        suffix=".bak",
        dir=detached_part_path.parent.parent,
    )
    os.close(descriptor)
    path = Path(backup_path)
    path.unlink()
    return path


def make_isolated_part_path(detached_part_path: Path, part_name: str) -> Path:
    path = Path(
        tempfile.mkdtemp(
            prefix=f".{part_name}.",
            suffix=".isolated",
            dir=detached_part_path.parent.parent,
        )
    )
    path.rmdir()
    return path


def isolate_detached_parts(
    detached_part_paths: Dict[str, Optional[Path]],
    excluded_parts: List[BrokenPartInfo],
) -> List[Tuple[Path, Path]]:
    isolated_parts: List[Tuple[Path, Path]] = []
    try:
        for part_info in excluded_parts:
            detached_part_path = detached_part_paths[part_info.name]
            assert detached_part_path is not None
            isolated_path = make_isolated_part_path(detached_part_path, part_info.name)
            detached_part_path.rename(isolated_path)
            isolated_parts.append((isolated_path, detached_part_path))
    except OSError:
        restore_isolated_part_paths(isolated_parts)
        raise
    return isolated_parts


def restore_isolated_part_paths(paths: List[Tuple[Path, Path]]) -> None:
    for isolated_path, detached_part_path in reversed(paths):
        if isolated_path.exists():
            isolated_path.rename(detached_part_path)


def cleanup_backup_path(backup_path: Optional[Path]) -> None:
    if backup_path is None:
        return
    if backup_path.exists():
        backup_path.unlink()


def restore_backup_paths(backups: List[Tuple[Path, Path]]) -> None:
    for backup_path, pointer_path in reversed(backups):
        if backup_path.exists():
            if pointer_path.exists():
                backup_path.unlink()
            else:
                backup_path.rename(pointer_path)


def make_restore_report(
    part_info: BrokenPartInfo,
    filename: str,
    status: str,
    detail: str,
) -> Dict[str, Any]:
    return {
        "table": part_info.table_id,
        "partition": part_info.partition_id,
        "part": part_info.name,
        "file": filename,
        "status": status,
        "detail": detail,
    }


def ensure_actions_are_safe(
    broken_parts: List[BrokenPart],
    parts_checked: int,
    max_broken_parts_percentage: float,
) -> None:
    if not broken_parts:
        return
    if parts_checked <= 0:
        raise ValueError(
            "Cannot calculate broken parts percentage without checked parts"
        )

    broken_parts_percentage = len(broken_parts) / parts_checked * 100
    if broken_parts_percentage > max_broken_parts_percentage:
        raise RuntimeError(
            "S3 check circuit breaker: "
            f"{len(broken_parts)} of {parts_checked} checked part(s) are broken "
            f"({broken_parts_percentage:.2f}%), threshold is "
            f"{max_broken_parts_percentage:.2f}%; no actions were performed"
        )


def select_actionable_broken_parts(
    broken_parts: List[BrokenPart],
) -> List[BrokenPart]:
    return [
        part
        for partition_parts in group_broken_parts_by_partition(broken_parts).values()
        if not any(
            issue.check.status == ObjectCheckStatus.UNKNOWN
            for part in partition_parts
            for issue in part.object_issues
        )
        for part in partition_parts
    ]


def make_partition_reports(
    broken_parts: List[BrokenPart],
    action_reports: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    actions = action_reports or []
    actions_by_file: Dict[Tuple[str, str, str, str], List[Dict[str, Any]]] = {}
    for action in actions:
        key = (
            action["table"],
            action["partition"],
            action["part"],
            action["file"],
        )
        actions_by_file.setdefault(key, []).append(action)

    result: List[Dict[str, Any]] = []
    for partition_key, partition_parts in sorted(
        group_broken_parts_by_partition(broken_parts).items()
    ):
        object_statuses = [
            issue.check.status
            for part in partition_parts
            for issue in part.object_issues
        ]
        partition_actions = [
            action
            for action in actions
            if action["table"] == partition_parts[0].info.table_id
            and action["partition"] == partition_key.partition_id
        ]
        part_reports = [
            _make_part_report(part, actions_by_file) for part in partition_parts
        ]
        result.append(
            {
                "database": partition_key.database,
                "table": partition_key.table,
                "partition_id": partition_key.partition_id,
                "check": _aggregate_check(object_statuses),
                "action": _aggregate_action(partition_actions),
                "parts_affected": len(partition_parts),
                "objects_checked": sum(
                    part.objects_checked for part in partition_parts
                ),
                "objects_missing": object_statuses.count(ObjectCheckStatus.MISSING),
                "objects_corrupted": object_statuses.count(ObjectCheckStatus.CORRUPTED),
                "objects_unknown": object_statuses.count(ObjectCheckStatus.UNKNOWN),
                "parts": part_reports,
            }
        )
    return result


def make_partition_table_row(report: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "database": report["database"],
        "table": report["table"],
        "partition_id": report["partition_id"],
        "check": report["check"],
        "action": report["action"],
        "parts_affected": report["parts_affected"],
        "objects_checked": report["objects_checked"],
        "objects_missing": report["objects_missing"],
        "objects_corrupted": report["objects_corrupted"],
        "objects_unknown": report["objects_unknown"],
    }


def _make_part_report(
    broken_part: BrokenPart,
    actions_by_file: Dict[Tuple[str, str, str, str], List[Dict[str, Any]]],
) -> Dict[str, Any]:
    issues_by_file: Dict[str, List[ObjectIssue]] = {}
    for issue in broken_part.object_issues:
        issues_by_file.setdefault(issue.filename, []).append(issue)

    file_reports = []
    for filename, issues in sorted(issues_by_file.items()):
        action_key = (
            broken_part.info.table_id,
            broken_part.info.partition_id,
            broken_part.info.name,
            filename,
        )
        file_actions = actions_by_file.get(action_key, [])
        file_report: Dict[str, Any] = {
            "name": filename,
            "check": _aggregate_check([issue.check.status for issue in issues]),
            "recoverable": _file_is_recoverable(broken_part.info, filename, issues),
            "action": _aggregate_action(file_actions),
        }
        if file_actions:
            file_report["action_detail"] = "; ".join(
                action["detail"] for action in file_actions
            )
        file_report["objects"] = [_make_object_report(issue) for issue in issues]
        file_reports.append(file_report)

    return {
        "name": broken_part.info.name,
        "check": _aggregate_check(
            [issue.check.status for issue in broken_part.object_issues]
        ),
        "objects_checked": broken_part.objects_checked,
        "files": file_reports,
    }


def _make_object_report(issue: ObjectIssue) -> Dict[str, Any]:
    check = issue.check
    report: Dict[str, Any] = {
        "key": check.key,
        "size": check.expected_size,
        "check": check.status.value,
        "reason": check.reason,
    }
    if check.read_size is not None:
        report["read_size"] = check.read_size
    if check.error:
        report["error"] = check.error
    return report


def _aggregate_check(statuses: List[ObjectCheckStatus]) -> str:
    if ObjectCheckStatus.UNKNOWN in statuses:
        return ObjectCheckStatus.UNKNOWN.value
    if ObjectCheckStatus.CORRUPTED in statuses:
        return ObjectCheckStatus.CORRUPTED.value
    if ObjectCheckStatus.MISSING in statuses:
        return ObjectCheckStatus.MISSING.value
    return ObjectCheckStatus.OK.value


def _aggregate_action(actions: List[Dict[str, Any]]) -> str:
    if not actions:
        return "none"
    statuses = [action["status"] for action in actions]
    partition_statuses = [
        status
        for status in statuses
        if status
        in {
            "detached",
            "detach_failed",
            "reattached",
            "reattach_failed",
        }
    ]
    if partition_statuses:
        return partition_statuses[-1]
    if all(status == "restored" for status in statuses):
        return "restored"
    if "restored" in statuses:
        return "partially-restored"
    return statuses[-1]


def _file_is_recoverable(
    part_info: BrokenPartInfo,
    filename: str,
    issues: List[ObjectIssue],
) -> bool:
    if any(issue.check.status != ObjectCheckStatus.MISSING for issue in issues):
        return False
    if all(issue.object_info.size == 0 for issue in issues):
        return True
    if filename == "columns.txt":
        return can_restore_columns_txt(part_info)
    return filename in {
        "checksums.txt",
        "count.txt",
        "default_compression_codec.txt",
    }


def get_active_parts_by_path(ctx: Context) -> Dict[str, BrokenPartInfo]:
    return {
        part["path"]: BrokenPartInfo(
            database=part["database"],
            table=part["table"],
            partition_id=part["partition_id"],
            name=part["name"],
            part_type=part["part_type"],
            rows=int(part["rows"]),
            default_compression_codec=part["default_compression_codec"],
        )
        for part in list_parts(ctx, active=True)
    }


def get_detached_part_path(ctx: Context, part_info: BrokenPartInfo) -> Optional[Path]:
    detached_parts = list_detached_parts(
        ctx,
        database=part_info.database,
        table=part_info.table,
        part_name=part_info.name,
        limit=1,
    )
    if not detached_parts:
        return None
    return Path(detached_parts[0]["path"].rstrip("/"))


def detach_partition_with_retry(ctx: Context, part_info: BrokenPartInfo) -> bool:
    return query_with_retry(
        lambda: detach_clickhouse_partition(
            ctx, part_info.database, part_info.table, part_info.partition_id
        ),
        f"DETACH PARTITION {part_info.table_id}.{part_info.partition_id}",
        retries=ATTACH_DETACH_QUERY_RETRY,
    )


def attach_partition_with_retry(ctx: Context, part_info: BrokenPartInfo) -> bool:
    return query_with_retry(
        lambda: attach_clickhouse_partition(
            ctx, part_info.database, part_info.table, part_info.partition_id
        ),
        f"ATTACH PARTITION {part_info.table_id}.{part_info.partition_id}",
        retries=2 * ATTACH_DETACH_QUERY_RETRY,
    )


def query_with_retry(
    action: Callable[[], None], description: str, retries: int
) -> bool:
    logging.debug("Execute query: {}", description)
    for retry in range(retries):
        try:
            action()
            logging.info("Query {} finished successfully", description)
            return True
        except Exception as e:
            if retry + 1 == retries:
                logging.warning("Query {} failed  with:  {!r}\n", description, e)
                return False

    return False
