"""Validate detached S3 parts before attaching their partitions."""

from __future__ import annotations

import os
import posixpath
from collections import OrderedDict, defaultdict
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, IncompleteReadError, ResponseStreamingError
from click import Context

from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    S3ObjectLocalMetaData,
    get_object_storage_key,
)
from ch_tools.chadmin.internal.part import (
    list_detached_parts,
    remove_detached_part_prefix_on_disk,
)
from ch_tools.chadmin.internal.partition import attach_partition
from ch_tools.chadmin.internal.table import get_table
from ch_tools.chadmin.internal.table_replica import get_table_replica
from ch_tools.chadmin.internal.zero_copy import (
    _get_zero_copy_zookeeper_path_for_disk_type,
    get_zero_copy_lock_unique_id,
)
from ch_tools.chadmin.internal.zookeeper import (
    delete_recursive,
    get_table_shared_id,
    zk_client,
)
from ch_tools.common import logging
from ch_tools.common.clickhouse.config import get_clickhouse_config
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration

READ_CHUNK_SIZE = 8 * 1024 * 1024
MISSING_OBJECT_ERROR_CODES = {"404", "NoSuchKey", "NotFound"}
MAX_REPORTED_ERRORS = 5
MAX_CACHED_OBJECT_PROBES = 10_000

# Keep this in sync with ClickHouse's DetachedPartInfo::DETACH_REASONS.  A
# custom prefix is not parsed as a detached-part reason reliably by ClickHouse.
DETACHED_PART_REASONS = (
    "broken",
    "unexpected",
    "noquorum",
    "ignored",
    "broken-on-start",
    "clone",
    "attaching",
    "deleting",
    "tmp-fetch",
    "covered-by-broken",
    "merge-not-byte-identical",
    "mutate-not-byte-identical",
    "broken-from-backup",
)


@dataclass(frozen=True, order=True)
class PartitionKey:
    database: str
    table: str
    partition_id: str


@dataclass(frozen=True)
class ObjectProbe:
    exists: bool
    reported_size: Optional[int] = None
    read_size: Optional[int] = None


@dataclass(frozen=True)
class ZeroCopyCleanupPlan:
    lock_roots: Tuple[str, ...]


@dataclass
class PartCheck:
    part: Dict[str, Any]
    passed: bool = True
    check_result: str = "not-run"
    objects_checked: int = 0
    bytes_checked: int = 0
    error: str = ""
    action: str = "pending"
    old_zero_copy_locks: str = "not-requested"
    quarantine_reason: Optional[str] = None
    renamed_to: str = ""

    @property
    def key(self) -> PartitionKey:
        return partition_key(self.part)

    def report(self) -> Dict[str, Any]:
        return {
            "database": self.part["database"],
            "table": self.part["table"],
            "partition_id": self.part["partition_id"],
            "part": self.part["name"],
            "reason": self.part["reason"],
            "disk": self.part["disk_name"],
            "path": self.part["path"],
            "check": self.check_result,
            "objects_checked": self.objects_checked,
            "bytes_checked": self.bytes_checked,
            "action": self.action,
            "old_zero_copy_locks": self.old_zero_copy_locks,
            "quarantine_reason": self.quarantine_reason or "",
            "destination_part": Path(self.renamed_to).name if self.renamed_to else "",
            "error": self.error,
        }


@dataclass(frozen=True, eq=False)
class PartRename:
    check: PartCheck
    source: str
    destination: str
    kind: str


class S3ObjectChecker:
    """Probe S3 objects, caching only results when parts share a remote blob."""

    def __init__(
        self,
        client: Any,
        bucket: str,
        prefix: str,
        method: str,
    ) -> None:
        if method not in {"head", "get"}:
            raise ValueError(f"Unsupported S3 check method: {method}")
        self.client = client
        self.bucket = bucket
        self.prefix = prefix
        self.method = method
        self.cache: OrderedDict[str, ObjectProbe] = OrderedDict()

    def check(self, object_info: Any) -> Optional[str]:
        key = get_object_storage_key(self.prefix, object_info)
        try:
            probe = self.cache.pop(key)
        except KeyError:
            probe = self._probe(key)
            if len(self.cache) >= MAX_CACHED_OBJECT_PROBES:
                self.cache.popitem(last=False)
        self.cache[key] = probe

        if not probe.exists:
            return f"S3 object is missing: {key}"
        if probe.reported_size != object_info.size:
            return (
                f"S3 object size mismatch for {key}: expected {object_info.size}, "
                f"reported {probe.reported_size}"
            )
        if self.method == "get" and probe.read_size != object_info.size:
            return (
                f"S3 object read size mismatch for {key}: expected "
                f"{object_info.size}, read {probe.read_size}"
            )
        return None

    def _probe(self, key: str) -> ObjectProbe:
        try:
            if self.method == "head":
                response = self.client.head_object(Bucket=self.bucket, Key=key)
                return ObjectProbe(
                    exists=True,
                    reported_size=_content_length(response),
                )

            response = self.client.get_object(Bucket=self.bucket, Key=key)
            body = response["Body"]
            read_size = 0
            try:
                while True:
                    chunk = body.read(READ_CHUNK_SIZE)
                    if not chunk:
                        break
                    read_size += len(chunk)
            finally:
                body.close()
            return ObjectProbe(
                exists=True,
                reported_size=_content_length(response),
                read_size=read_size,
            )
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code", "")
            if code in MISSING_OBJECT_ERROR_CODES:
                return ObjectProbe(exists=False)
            raise


def attach_checked_partitions(
    ctx: Context,
    selected_partitions: Iterable[Dict[str, Any]],
    reason: str,
    check_method: Optional[str],
    dry_run: bool,
    keep_going: bool,
    cleanup_old_zero_copy_locks: bool = False,
    quarantine_failed_unprefixed: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Check selected detached parts and attach only safe partition contents."""
    if cleanup_old_zero_copy_locks and not reason:
        raise ValueError("Old zero-copy locks can only be cleaned with --reason")
    if check_method not in {None, "head", "get"}:
        raise ValueError(f"Unsupported S3 check method: {check_method}")
    if (
        quarantine_failed_unprefixed is not None
        and quarantine_failed_unprefixed not in DETACHED_PART_REASONS
    ):
        raise ValueError(
            "Unsupported detached-part reason for "
            f"--quarantine-failed-unprefixed: {quarantine_failed_unprefixed}"
        )
    if quarantine_failed_unprefixed is not None and check_method is None:
        raise ValueError("--quarantine-failed-unprefixed requires --check-s3-objects")

    selected_keys = {partition_key(item) for item in selected_partitions}
    if not selected_keys:
        return []

    parts = _parts_in_partitions(ctx, selected_keys, reason)
    checks = [PartCheck(part=part) for part in parts]
    checks_by_partition = _group_checks_by_partition(checks)

    if check_method is not None:
        _run_s3_checks(ctx, checks, check_method, keep_going)

    for check in checks:
        if not check.passed:
            check.action = (
                "left-prefixed"
                if reason and check.part["reason"] == reason
                else "skipped"
            )
    _preflight_part_renames(
        checks,
        reason,
        quarantine_failed_unprefixed,
        keep_going,
    )

    cleanup_plans: Dict[PartitionKey, ZeroCopyCleanupPlan] = {}
    if cleanup_old_zero_copy_locks:
        for check in checks:
            check.old_zero_copy_locks = "not-deleted"
        cleanup_plans = _prepare_zero_copy_cleanup_plans(
            ctx, checks_by_partition, keep_going
        )

    keeper = zk_client(ctx) if cleanup_plans else nullcontext(None)
    with keeper as zk:
        _attach_ready_partitions(
            ctx,
            checks_by_partition,
            reason,
            dry_run,
            keep_going,
            cleanup_plans,
            zk,
        )

    return [check.report() for check in checks]


def partition_key(item: Dict[str, Any]) -> PartitionKey:
    return PartitionKey(item["database"], item["table"], item["partition_id"])


def _zero_copy_part_name(part: Dict[str, Any]) -> str:
    """Return the logical part name used in ClickHouse zero-copy lock paths."""
    name = part["name"]
    reason = part["reason"]
    if not reason:
        return name

    prefix = f"{reason}_"
    if not name.startswith(prefix):
        raise RuntimeError(
            "Detached part name does not match its reason: "
            f"name={name!r}, reason={reason!r}"
        )
    return name[len(prefix) :]


def _parts_in_partitions(
    ctx: Context, selected_keys: set[PartitionKey], reason: str
) -> List[Dict[str, Any]]:
    reasons = (reason, "") if reason else ("",)
    parts: List[Dict[str, Any]] = []
    for selected_reason in reasons:
        parts.extend(
            part
            for part in list_detached_parts(ctx, reason=selected_reason)
            if partition_key(part) in selected_keys
            and part["reason"] == selected_reason
        )
    if any(not part.get("path") for part in parts):
        raise RuntimeError(
            "Checked partition attach requires system.detached_parts.path, "
            "which is available since ClickHouse 23.1"
        )
    return parts


def _run_s3_checks(
    ctx: Context,
    checks: List[PartCheck],
    check_method: str,
    keep_going: bool,
) -> None:
    checkers: Dict[str, S3ObjectChecker] = {}
    for check in checks:
        try:
            checker = checkers.get(check.part["disk_name"])
            if checker is None:
                checker = _make_s3_checker(ctx, check.part["disk_name"], check_method)
                checkers[check.part["disk_name"]] = checker
            _check_part(check, checker)
        except Exception as error:
            if not keep_going:
                raise
            check.passed = False
            check.check_result = "failed"
            check.action = "skipped"
            check.error = f"S3 check error: {error!r}"
            logging.warning(
                "Failed to check detached part {}: {!r}",
                check.part["path"],
                error,
            )


def _prepare_zero_copy_cleanup_plans(
    ctx: Context,
    checks_by_partition: Dict[PartitionKey, List[PartCheck]],
    keep_going: bool,
) -> Dict[PartitionKey, ZeroCopyCleanupPlan]:
    storage_configuration = get_clickhouse_config(ctx).storage_configuration
    table_ids: Dict[Tuple[str, str], Tuple[str, str]] = {}
    disk_configs: Dict[str, S3DiskConfiguration] = {}
    zero_copy_paths: Dict[Tuple[str, str], str] = {}
    plans: Dict[PartitionKey, ZeroCopyCleanupPlan] = {}

    for key in sorted(checks_by_partition):
        partition_checks = checks_by_partition[key]
        if _has_failed_unprefixed_part(partition_checks):
            continue
        ready = [check for check in partition_checks if check.passed]
        if not ready:
            continue
        try:
            table_key = (key.database, key.table)
            if table_key not in table_ids:
                table = get_table(ctx, key.database, key.table)
                replica = get_table_replica(ctx, key.database, key.table)
                shared_table_id = get_table_shared_id(ctx, replica["zookeeper_path"])
                if not shared_table_id:
                    raise RuntimeError(
                        f"Table {key.database}.{key.table} has no table_shared_id"
                    )
                table_ids[table_key] = (table["uuid"], shared_table_id)

            table_uuid, shared_table_id = table_ids[table_key]
            lock_roots: Set[str] = set()
            for check in ready:
                disk_name = check.part["disk_name"]
                if disk_name not in disk_configs:
                    disk_configs[disk_name] = S3DiskConfiguration.from_config(
                        storage_configuration,
                        disk_name,
                        ctx.obj["config"]["object_storage"]["bucket_name_prefix"],
                    )
                disk_config = disk_configs[disk_name]

                path_key = (key.database, key.table)
                if path_key not in zero_copy_paths:
                    zero_copy_paths[path_key] = (
                        _get_zero_copy_zookeeper_path_for_disk_type(
                            ctx,
                            disk_config.OBJECT_STORAGE_TYPE,
                            table_uuid=table_uuid,
                        )
                    )
                unique_id = get_zero_copy_lock_unique_id(
                    disk_config.prefix,
                    check.part,
                )
                lock_roots.add(
                    posixpath.join(
                        zero_copy_paths[path_key],
                        shared_table_id,
                        _zero_copy_part_name(check.part),
                        unique_id,
                    )
                )

            plans[key] = ZeroCopyCleanupPlan(tuple(sorted(lock_roots)))
            for check in ready:
                check.old_zero_copy_locks = "pending"
        except Exception as error:
            if not keep_going:
                raise
            message = f"Cannot prepare old zero-copy lock cleanup: {error!r}"
            for check in ready:
                check.passed = False
                check.action = "left-prefixed" if check.part["reason"] else "skipped"
                check.old_zero_copy_locks = "prepare-failed"
                _append_error(check, message)
            logging.warning(
                "Cannot prepare old zero-copy lock cleanup for {}.{}:{}: {!r}",
                key.database,
                key.table,
                key.partition_id,
                error,
            )

    return plans


def _attach_ready_partitions(
    ctx: Context,
    checks_by_partition: Dict[PartitionKey, List[PartCheck]],
    reason: str,
    dry_run: bool,
    keep_going: bool,
    cleanup_plans: Dict[PartitionKey, ZeroCopyCleanupPlan],
    zk: Any,
) -> None:
    for key in sorted(checks_by_partition):
        partition_checks = checks_by_partition[key]
        ready = [check for check in partition_checks if check.passed]
        if _has_failed_unprefixed_part(partition_checks):
            for check in ready:
                check.action = "left-prefixed" if check.part["reason"] else "skipped"
                _append_error(
                    check,
                    "an unprefixed part in the partition failed the S3 check",
                )
                if check.old_zero_copy_locks == "pending":
                    check.old_zero_copy_locks = "not-deleted"
            continue
        if not ready:
            for check in partition_checks:
                check.quarantine_reason = None
            continue
        attached = _attach_partition(
            ctx,
            key,
            partition_checks,
            reason=reason,
            dry_run=dry_run,
            keep_going=keep_going,
        )
        if not attached:
            continue

        cleanup_plan = cleanup_plans.get(key)
        if cleanup_plan is not None:
            cleanup_checks = [
                check for check in ready if check.old_zero_copy_locks == "pending"
            ]
            _delete_old_zero_copy_locks(
                zk,
                key,
                cleanup_checks,
                cleanup_plan,
                dry_run,
                keep_going,
            )


def _has_failed_unprefixed_part(checks: Iterable[PartCheck]) -> bool:
    return any(
        not check.passed
        and not check.part["reason"]
        and check.quarantine_reason is None
        for check in checks
    )


def _group_checks_by_partition(
    checks: Iterable[PartCheck],
) -> Dict[PartitionKey, List[PartCheck]]:
    grouped: Dict[PartitionKey, List[PartCheck]] = defaultdict(list)
    for check in checks:
        grouped[check.key].append(check)
    return grouped


def _delete_old_zero_copy_locks(
    zk: Any,
    key: PartitionKey,
    checks: List[PartCheck],
    plan: ZeroCopyCleanupPlan,
    dry_run: bool,
    keep_going: bool,
) -> None:
    try:
        delete_recursive(zk, list(plan.lock_roots), dry_run=dry_run)
        status = "would-delete" if dry_run else "deleted"
        for check in checks:
            check.old_zero_copy_locks = status
    except Exception as error:
        message = (
            f"Partition attached, but old zero-copy lock cleanup failed: {error!r}"
        )
        for check in checks:
            check.old_zero_copy_locks = "delete-failed"
            _append_error(check, message)
        logging.error(
            "Old zero-copy lock cleanup failed after attaching {}.{}:{}: {!r}",
            key.database,
            key.table,
            key.partition_id,
            error,
        )
        if not keep_going:
            raise


def _append_error(check: PartCheck, message: str) -> None:
    check.error = f"{check.error}; {message}" if check.error else message


def _make_s3_checker(ctx: Context, disk_name: str, method: str) -> S3ObjectChecker:
    ch_config = get_clickhouse_config(ctx)
    disk_conf = S3DiskConfiguration.from_config(
        ch_config.storage_configuration,
        disk_name,
        ctx.obj["config"]["object_storage"]["bucket_name_prefix"],
    )
    retries = ctx.obj["config"]["object_storage"].get("retries")
    boto_config_args: Dict[str, Any] = {"s3": {"addressing_style": "auto"}}
    if retries is not None:
        boto_config_args["retries"] = retries
    client = boto3.client(
        "s3",
        endpoint_url=disk_conf.endpoint_url,
        aws_access_key_id=disk_conf.access_key_id,
        aws_secret_access_key=disk_conf.secret_access_key,
        config=Config(**boto_config_args),
    )
    return S3ObjectChecker(
        client,
        disk_conf.bucket_name,
        disk_conf.prefix,
        method,
    )


def _check_part(  # pylint: disable=too-many-branches
    check: PartCheck, checker: S3ObjectChecker, *, strict_metadata: bool = False
) -> None:
    part_path = Path(check.part["path"])
    errors: List[str] = []
    object_errors: List[str] = []
    if not part_path.is_dir() or part_path.is_symlink():
        check.passed = False
        check.check_result = "failed"
        check.action = "skipped"
        check.error = f"part path is not a regular directory: {part_path}"
        if strict_metadata:
            raise ValueError(check.error)
        return

    files_found = False
    walk_errors: List[OSError] = []
    for root, directories, filenames in os.walk(
        part_path,
        followlinks=False,
        onerror=walk_errors.append,
    ):
        root_path = Path(root)
        for directory in directories:
            if (root_path / directory).is_symlink():
                errors.append(f"symlinked directory: {root_path / directory}")
        for filename in filenames:
            files_found = True
            metadata_path = root_path / filename
            if metadata_path.is_symlink() or not metadata_path.is_file():
                errors.append(f"not a regular metadata file: {metadata_path}")
                continue
            try:
                metadata = S3ObjectLocalMetaData.from_file(metadata_path)
            except (OSError, ValueError, IndexError) as error:
                errors.append(f"cannot parse {metadata_path}: {error}")
                continue

            object_sizes = sum(item.size for item in metadata.objects)
            if object_sizes != metadata.total_size:
                errors.append(
                    f"metadata size mismatch in {metadata_path}: total_size "
                    f"{metadata.total_size}, objects {object_sizes}"
                )
            for object_info in metadata.objects:
                check.objects_checked += 1
                check.bytes_checked += object_info.size
                try:
                    object_error = checker.check(object_info)
                except (IncompleteReadError, ResponseStreamingError) as error:
                    if not strict_metadata:
                        raise
                    key = get_object_storage_key(checker.prefix, object_info)
                    object_error = f"Cannot read S3 object {key}: {error}"
                if object_error is not None:
                    (object_errors if strict_metadata else errors).append(object_error)

    errors.extend(f"cannot traverse {part_path}: {error}" for error in walk_errors)
    if not files_found:
        errors.append(f"part directory contains no metadata files: {part_path}")
    if strict_metadata and errors:
        raise ValueError(_summarize_errors(errors))
    errors.extend(object_errors)
    if errors:
        check.passed = False
        check.check_result = "failed"
        check.error = _summarize_errors(errors)
    else:
        check.check_result = "passed"


def _summarize_errors(errors: List[str]) -> str:
    displayed = errors[:MAX_REPORTED_ERRORS]
    if len(errors) > MAX_REPORTED_ERRORS:
        displayed.append(f"and {len(errors) - MAX_REPORTED_ERRORS} more error(s)")
    return "; ".join(displayed)


def _preflight_part_renames(
    checks: List[PartCheck],
    reason: str,
    quarantine_failed_unprefixed: Optional[str],
    keep_going: bool,
) -> None:
    """Validate all local renames before changing a detached part directory.

    A failed unprefixed part may occupy the destination of a selected prefixed
    part.  It is safe when the failed part is itself moved to quarantine first,
    so planned sources are not treated as conflicting destinations.
    """
    preflight_checks = checks
    if quarantine_failed_unprefixed is None:
        blocked_partitions = {
            check.key
            for check in checks
            if not check.passed and not check.part["reason"]
        }
        preflight_checks = [
            check for check in checks if check.key not in blocked_partitions
        ]
    unavailable_quarantine_checks: Set[int] = set()

    while True:
        plans = _part_renames(
            preflight_checks,
            reason,
            quarantine_failed_unprefixed,
            unavailable_quarantine_checks,
        )
        errors = _rename_preflight_errors(plans)
        if not errors:
            for plan in plans:
                if plan.kind == "quarantine":
                    plan.check.quarantine_reason = quarantine_failed_unprefixed
            return

        if not keep_going:
            raise RuntimeError(next(iter(errors.values())))

        for plan, message in errors.items():
            _mark_rename_unavailable(
                plan,
                message,
                unavailable_quarantine_checks,
            )


def _part_renames(
    checks: Iterable[PartCheck],
    reason: str,
    quarantine_failed_unprefixed: Optional[str] = None,
    unavailable_quarantine_checks: Optional[Set[int]] = None,
) -> List[PartRename]:
    if unavailable_quarantine_checks is None:
        unavailable_quarantine_checks = set()
    plans: List[PartRename] = []
    for check in checks:
        source = check.part["path"]
        if reason and check.passed and check.part["reason"] == reason:
            plans.append(
                PartRename(
                    check=check,
                    source=source,
                    destination=_destination_path(source, reason),
                    kind="unprefix",
                )
            )
        elif (
            # During preflight, propose quarantining each failed unprefixed
            # part. A failed proposal is excluded after --keep-going has
            # reported its collision, otherwise preflight would retry it
            # forever on the next iteration.
            quarantine_failed_unprefixed is not None
            and not check.passed
            and not check.part["reason"]
            and id(check) not in unavailable_quarantine_checks
        ):
            plans.append(
                PartRename(
                    check=check,
                    source=source,
                    destination=_quarantine_destination(
                        source,
                        quarantine_failed_unprefixed,
                    ),
                    kind="quarantine",
                )
            )
        elif check.quarantine_reason is not None:
            # During execution the requested reason is not passed again;
            # use only the quarantine rename approved by preflight.
            plans.append(
                PartRename(
                    check=check,
                    source=source,
                    destination=_quarantine_destination(
                        source,
                        check.quarantine_reason,
                    ),
                    kind="quarantine",
                )
            )
    return plans


def _rename_preflight_errors(
    plans: Iterable[PartRename],
) -> Dict[PartRename, str]:
    plans = list(plans)
    errors: Dict[PartRename, str] = {}
    sources = {plan.source for plan in plans}
    plans_by_source = {plan.source: plan for plan in plans}
    destinations: Dict[str, List[PartRename]] = defaultdict(list)
    for plan in plans:
        if not os.path.lexists(plan.source):
            errors[plan] = f"Detached part source does not exist: {plan.source}"
        destinations[plan.destination].append(plan)

        next_plan = plans_by_source.get(plan.destination)
        if next_plan is not None and next_plan.destination == plan.source:
            message = (
                "Cannot exchange detached part names while quarantining: "
                f"{plan.source} <-> {plan.destination}"
            )
            errors[plan] = message
            errors[next_plan] = message

    for destination, destination_plans in destinations.items():
        if len(destination_plans) > 1:
            message = (
                "Multiple detached parts have the same rename destination: "
                f"{destination}"
            )
            errors.update({plan: message for plan in destination_plans})
        elif os.path.lexists(destination) and destination not in sources:
            errors[destination_plans[0]] = (
                f"Detached part destination already exists: {destination}"
            )
    return errors


def _mark_rename_unavailable(
    plan: PartRename,
    message: str,
    unavailable_quarantine_checks: Set[int],
) -> None:
    check = plan.check
    if plan.kind == "quarantine":
        unavailable_quarantine_checks.add(id(check))
    else:
        check.passed = False
    check.action = "left-prefixed" if check.part["reason"] else "skipped"
    _append_error(check, message)
    logging.warning(message)


def _destination_path(part_path: str, reason: str) -> str:
    source = Path(part_path)
    prefix = f"{reason}_"
    if not source.name.startswith(prefix):
        raise RuntimeError(f"Unexpected detached part prefix: {part_path}")
    return str(source.with_name(source.name[len(prefix) :]))


def _quarantine_destination(part_path: str, reason: str) -> str:
    source = Path(part_path)
    return str(source.with_name(f"{reason}_{source.name}"))


def _attach_partition(
    ctx: Context,
    key: PartitionKey,
    checks: List[PartCheck],
    reason: str,
    dry_run: bool,
    keep_going: bool,
) -> bool:
    renamed: List[PartRename] = []
    try:
        plans = _part_renames(checks, reason)
        for plan in sorted(plans, key=lambda plan: plan.kind != "quarantine"):
            _apply_part_rename(plan, dry_run)
            renamed.append(plan)

        attach_partition(
            ctx,
            database=key.database,
            table=key.table,
            partition_id=key.partition_id,
            dry_run=dry_run,
        )
        for check in checks:
            if check.quarantine_reason is not None:
                check.action = "would-quarantine" if dry_run else "quarantined"
            elif check.passed:
                check.action = "would-attach" if dry_run else "attached"
        return True
    except Exception as error:
        if not dry_run:
            _restore_prefixes(renamed)
        for check in checks:
            if check.passed or check.quarantine_reason is not None:
                check.action = "attach-failed"
                check.renamed_to = ""
                if check.old_zero_copy_locks == "pending":
                    check.old_zero_copy_locks = "not-deleted"
                check.error = f"ATTACH PARTITION failed: {error!r}"
        if keep_going:
            logging.warning(
                "Failed to attach partition {}.{}:{}: {!r}",
                key.database,
                key.table,
                key.partition_id,
                error,
            )
            return False
        raise


def _apply_part_rename(plan: PartRename, dry_run: bool) -> None:
    if plan.kind == "unprefix":
        reason = plan.check.part["reason"]
        remove_detached_part_prefix_on_disk(
            plan.source,
            f"{reason}_",
            dry_run=dry_run,
            log_move=False,
        )
    elif not dry_run:
        os.rename(plan.source, plan.destination)
    plan.check.renamed_to = plan.destination


def _restore_prefixes(renamed: List[PartRename]) -> None:
    for plan in reversed(renamed):
        if not os.path.lexists(plan.destination):
            continue
        if os.path.lexists(plan.source):
            logging.error(
                "Cannot restore detached prefix because both paths exist: {} and {}",
                plan.source,
                plan.destination,
            )
            continue
        try:
            os.rename(plan.destination, plan.source)
        except OSError as error:
            logging.error(
                "Cannot restore detached prefix {} -> {}: {!r}",
                plan.destination,
                plan.source,
                error,
            )


def _content_length(response: Dict[str, Any]) -> Optional[int]:
    value = response.get("ContentLength")
    return int(value) if value is not None else None
