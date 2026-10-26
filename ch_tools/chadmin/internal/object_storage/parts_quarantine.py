"""Offline quarantine of object-storage MergeTree parts."""

from __future__ import annotations

import json
import os
import posixpath
import re
import socket
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set
from uuid import UUID

from click import Context
from kazoo.client import KazooClient

from ch_tools.chadmin.internal.system import ensure_clickhouse_server_stopped
from ch_tools.chadmin.internal.table_metadata_parser import (
    UUID_PATTERN,
    TableMetadataParser,
)
from ch_tools.chadmin.internal.utils import replace_macros
from ch_tools.chadmin.internal.zookeeper import (
    delete_nodes_batched,
    get_children,
    zk_client,
)
from ch_tools.common import logging
from ch_tools.common.clickhouse.config import get_clickhouse_config, get_macros
from ch_tools.common.utils import unescape_for_file_name

DETACHED_PREFIX = "broken-on-start_"
CLEAR_OLD_BROKEN_DETACHED_SETTING = "merge_tree_enable_clear_old_broken_detached"
PART_NAME_RE = re.compile(r"^.+_-?[0-9]+_-?[0-9]+_[0-9]+(?:_[0-9]+)?$")
CLEAR_OLD_BROKEN_DETACHED_RE = re.compile(
    rf"\b{CLEAR_OLD_BROKEN_DETACHED_SETTING}\s*=\s*(?:1|true)\b",
    re.IGNORECASE,
)
REPLICATED_DATABASE_RE = re.compile(
    r"\bENGINE\s*=\s*Replicated\s*\(\s*'[^']*'\s*,\s*'([^']*)'\s*,\s*'([^']*)'",
    re.DOTALL,
)


class ReplicaIdentityError(ValueError):
    """The local ReplicatedMergeTree replica cannot be identified offline."""


@dataclass(frozen=True)
class OfflineTable:
    """Local metadata required to map a table UUID to its Keeper root."""

    database: str
    table: str
    uuid: str
    zookeeper_path: Optional[str]
    replica_name: Optional[str]

    @property
    def display_name(self) -> str:
        return f"{self.database}.{self.table}"


@dataclass
class QuarantinedPart:
    """One local part directory included in the quarantine operation."""

    table_uuid: str
    part_name: str
    source_path: Path
    detached_path: Path
    temporary: bool
    already_detached: bool
    table: Optional[OfflineTable] = None
    zookeeper_nodes: List[str] = field(default_factory=list)

    @property
    def local_action(self) -> str:
        return "already_detached" if self.already_detached else "move"


@dataclass(frozen=True)
class QuarantineReport:
    """Result returned to the CLI."""

    parts: List[QuarantinedPart]
    moved: int
    already_detached: int
    zookeeper_nodes: int
    dry_run: bool
    local_replicas: List[str] = field(default_factory=list)
    replicas_to_mark_lost: List[str] = field(default_factory=list)

    def summary(self) -> Dict[str, Any]:
        return {
            "parts": len(self.parts),
            "moved": self.moved,
            "already_detached": self.already_detached,
            "zookeeper_nodes": self.zookeeper_nodes,
            "local_replicas": self.local_replicas,
            "replicas_to_mark_lost": self.replicas_to_mark_lost,
            "dry_run": self.dry_run,
        }

    def details(self) -> List[Dict[str, Any]]:
        result = []
        for part in self.parts:
            result.append(
                {
                    "database": part.table.database if part.table else None,
                    "table": part.table.table if part.table else None,
                    "part": part.part_name,
                    "temporary": part.temporary,
                    "local_action": part.local_action,
                    "zookeeper_nodes": len(part.zookeeper_nodes),
                    "source": str(part.source_path),
                    "destination": str(part.detached_path),
                }
            )
        return result


@dataclass(frozen=True)
class ReplicaLostPlan:
    """Keeper nodes and human-readable replica names for repair-mode marking."""

    local_replicas: List[str]
    replicas_to_mark_lost: List[str]
    nodes_to_mark_lost: List[str]


def _setting_is_enabled(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"", "0", "false", "no", "off"}:
            return False
    raise ValueError(
        f"Cannot interpret ClickHouse setting value as a boolean: {value!r}"
    )


def ensure_broken_detached_parts_are_not_cleaned(ctx: Context) -> None:
    """Protect quarantined parts from ClickHouse's detached-part cleanup."""
    settings = get_clickhouse_config(ctx).merge_tree_settings
    value = settings.get(CLEAR_OLD_BROKEN_DETACHED_SETTING, 0)
    if _setting_is_enabled(value):
        raise RuntimeError(
            f"ClickHouse setting {CLEAR_OLD_BROKEN_DETACHED_SETTING} is enabled; "
            "it can delete quarantined parts"
        )


def _valid_table_uuid(table_dir: Path) -> bool:
    try:
        parsed = UUID(table_dir.name)
    except ValueError:
        return False
    return str(parsed) == table_dir.name and table_dir.parent.name == table_dir.name[:3]


def _is_part(path: Path, part_name: str) -> bool:
    if part_name.startswith("tmp_"):
        return True
    return bool(PART_NAME_RE.fullmatch(part_name)) and (
        (path / "checksums.txt").is_file() and (path / "columns.txt").is_file()
    )


def _detached_part_name(path: Path, detached_prefix: str) -> Optional[str]:
    candidate = path.name[len(detached_prefix) :]
    retry_match = re.fullmatch(r"(.+)_try[1-9][0-9]*", candidate)
    if retry_match is not None and _is_part(path, retry_match.group(1)):
        return retry_match.group(1)
    if _is_part(path, candidate):
        return candidate
    return None


def discover_object_storage_parts(
    root_path: Path, detached_prefix: str = DETACHED_PREFIX
) -> List[QuarantinedPart]:
    """Discover active, temporary, and already quarantined part directories."""
    if not root_path.is_dir():
        raise ValueError(f"Object-storage store directory does not exist: {root_path}")
    if not detached_prefix or "/" in detached_prefix or detached_prefix in {".", ".."}:
        raise ValueError(f"Invalid detached part prefix: {detached_prefix!r}")

    result: List[QuarantinedPart] = []
    for prefix_dir in sorted(root_path.iterdir()):
        if not prefix_dir.is_dir() or prefix_dir.is_symlink():
            continue
        for table_dir in sorted(prefix_dir.iterdir()):
            if (
                not table_dir.is_dir()
                or table_dir.is_symlink()
                or not _valid_table_uuid(table_dir)
            ):
                continue

            detached_dir = table_dir / "detached"
            if not detached_dir.is_dir() or detached_dir.is_symlink():
                raise ValueError(
                    f"Detached directory does not exist or is not a real directory: "
                    f"{detached_dir}"
                )
            for source_path in sorted(table_dir.iterdir()):
                if (
                    not source_path.is_dir()
                    or source_path.is_symlink()
                    or source_path.name == "detached"
                    or not _is_part(source_path, source_path.name)
                ):
                    continue
                result.append(
                    QuarantinedPart(
                        table_uuid=table_dir.name,
                        part_name=source_path.name,
                        source_path=source_path,
                        detached_path=detached_dir
                        / f"{detached_prefix}{source_path.name}",
                        temporary=source_path.name.startswith("tmp_"),
                        already_detached=False,
                    )
                )

            for detached_path in sorted(detached_dir.iterdir()):
                if not detached_path.is_dir() or detached_path.is_symlink():
                    continue
                if not detached_path.name.startswith(detached_prefix):
                    continue
                part_name = _detached_part_name(detached_path, detached_prefix)
                if part_name is None:
                    continue
                result.append(
                    QuarantinedPart(
                        table_uuid=table_dir.name,
                        part_name=part_name,
                        source_path=table_dir / part_name,
                        detached_path=detached_path,
                        temporary=part_name.startswith("tmp_"),
                        already_detached=True,
                    )
                )

    return result


def _expand_zookeeper_path(
    path: str, table_uuid: str, database: str, table: str, macros: Dict[str, str]
) -> str:
    expanded = replace_macros(
        path,
        {
            **macros,
            "uuid": table_uuid,
            "database": database,
            "table": table,
        },
    )
    if re.search(r"{[^{}]+}", expanded):
        raise ValueError(
            f"Cannot expand all macros in ReplicatedMergeTree path {path!r}: {expanded!r}"
        )
    if not expanded.startswith("/"):
        raise ValueError(f"ReplicatedMergeTree path must be absolute: {expanded!r}")
    return posixpath.normpath(expanded)


def _replica_identity_error(table: str, reason: str) -> ReplicaIdentityError:
    return ReplicaIdentityError(
        f"Cannot determine the local replica for {table}: {reason}. "
        "Fix the local metadata/configuration or rerun with "
        "--no-mark-replicas-lost"
    )


def _expand_replica_name(
    replica_name: str,
    table_uuid: str,
    database: str,
    table: str,
    macros: Dict[str, str],
) -> str:
    expanded = replace_macros(
        replica_name,
        {
            **macros,
            "uuid": table_uuid,
            "database": database,
            "table": table,
        },
    )
    if re.search(r"{[^{}]+}", expanded):
        raise ValueError(
            f"replica name {replica_name!r} contains unresolved macros: {expanded!r}"
        )
    if not expanded or "/" in expanded or expanded in {".", ".."}:
        raise ValueError(f"invalid replica name after macro expansion: {expanded!r}")
    return expanded


def _database_replica_macros(
    metadata_path: Path,
    escaped_database: str,
    macros: Dict[str, str],
) -> Dict[str, str]:
    """Return table macro overrides supplied by a Replicated database."""
    database_metadata_path = metadata_path / f"{escaped_database}.sql"
    try:
        database_metadata = database_metadata_path.read_text(encoding="utf-8")
    except OSError as error:
        raise ValueError(
            f"cannot read database metadata {database_metadata_path}: {error}"
        ) from error

    match = REPLICATED_DATABASE_RE.search(database_metadata)
    if match is None:
        return macros

    shard_template, replica_template = match.groups()
    database = unescape_for_file_name(escaped_database)
    special_macros = {
        **macros,
        "database": database,
    }
    shard = replace_macros(shard_template, special_macros)
    replica = replace_macros(replica_template, special_macros)
    for name, value in (("shard", shard), ("replica", replica)):
        if re.search(r"{[^{}]+}", value):
            raise ValueError(
                f"Replicated database {database} has unresolved {name} "
                f"macros: {value!r}"
            )
    return {**macros, "shard": shard, "replica": replica}


def _load_offline_table(
    metadata_path: Path,
    metadata_file: Path,
    macros: Dict[str, str],
    default_replica_path: Optional[str],
    default_replica_name: Optional[str],
    require_replica_name: bool,
) -> OfflineTable:
    """Parse one table and resolve its effective offline replica identity."""
    metadata = TableMetadataParser.parse(str(metadata_file))
    database = unescape_for_file_name(metadata_file.parent.name)
    table = unescape_for_file_name(metadata_file.stem)
    if not metadata.is_replicated():
        return OfflineTable(database, table, metadata.table_uuid, None, None)

    # Replicated databases override {shard}/{replica} for their tables. Apply
    # those macros to both engine arguments, not only to the replica name.
    replica_macros = _database_replica_macros(
        metadata_path,
        metadata_file.parent.name,
        macros,
    )
    replica_path = metadata.effective_replica_path(default_replica_path)
    zookeeper_path = _expand_zookeeper_path(
        replica_path,
        metadata.table_uuid,
        database,
        table,
        replica_macros,
    )

    replica_template = metadata.replica_name or default_replica_name
    if replica_template is None:
        if require_replica_name:
            raise _replica_identity_error(
                f"{database}.{table}",
                "neither table metadata nor server configuration contains a "
                "replica name",
            )
        replica_name = None
    else:
        try:
            replica_name = _expand_replica_name(
                replica_template,
                metadata.table_uuid,
                database,
                table,
                replica_macros,
            )
        except Exception as error:
            if require_replica_name:
                raise _replica_identity_error(
                    f"{database}.{table}", str(error)
                ) from error
            logging.warning(
                "Cannot determine the local replica for {}.{}: {!r}; "
                "replicas will not be marked lost for this table",
                database,
                table,
                error,
            )
            replica_name = None

    return OfflineTable(
        database=database,
        table=table,
        uuid=metadata.table_uuid,
        zookeeper_path=zookeeper_path,
        replica_name=replica_name,
    )


def load_offline_tables(
    metadata_path: Path,
    table_uuids: Set[str],
    macros: Dict[str, str],
    default_replica_path: Optional[str] = None,
    default_replica_name: Optional[str] = None,
    require_replica_name: bool = False,
    keep_going: bool = False,
) -> Dict[str, OfflineTable]:
    """Load only metadata files that own discovered object-storage directories."""
    if not metadata_path.is_dir():
        message = f"ClickHouse metadata directory does not exist: {metadata_path}"
        if not keep_going:
            raise ValueError(message)
        logging.warning("{}; the local parts will still be detached", message)
        return {}

    tables: Dict[str, OfflineTable] = {}
    invalid_uuids: Set[str] = set()
    for metadata_file in sorted(metadata_path.glob("*/*.sql")):
        try:
            metadata_text = metadata_file.read_text(encoding="utf-8")
        except OSError as error:
            if not keep_going:
                raise ValueError(
                    f"Cannot read table metadata {metadata_file}: {error}"
                ) from error
            logging.warning("Cannot read table metadata {}: {!r}", metadata_file, error)
            continue
        uuid_match = UUID_PATTERN.search(metadata_text)
        if uuid_match is None or uuid_match.group(1) not in table_uuids:
            continue

        table_uuid = uuid_match.group(1)
        if table_uuid in invalid_uuids:
            continue
        if CLEAR_OLD_BROKEN_DETACHED_RE.search(metadata_text):
            raise RuntimeError(
                f"Table metadata {metadata_file} enables "
                f"{CLEAR_OLD_BROKEN_DETACHED_SETTING}; it can delete "
                "quarantined parts"
            )
        try:
            offline_table = _load_offline_table(
                metadata_path,
                metadata_file,
                macros,
                default_replica_path,
                default_replica_name,
                require_replica_name,
            )
            previous = tables.setdefault(offline_table.uuid, offline_table)
            if previous != offline_table:
                raise ValueError(
                    f"Table UUID {offline_table.uuid} belongs to multiple metadata files"
                )
        except Exception as error:
            if isinstance(error, ReplicaIdentityError):
                raise
            if not keep_going:
                raise
            tables.pop(table_uuid, None)
            invalid_uuids.add(table_uuid)
            logging.warning(
                "Cannot use metadata {} for table UUID {}: {!r}; "
                "the local parts will still be detached",
                metadata_file,
                table_uuid,
                error,
            )

    for table_uuid in invalid_uuids:
        tables.pop(table_uuid, None)

    missing = sorted(table_uuids - set(tables))
    if missing:
        message = (
            "Cannot map object-storage table UUIDs to local metadata: "
            + ", ".join(missing)
        )
        if not keep_going:
            raise ValueError(message)
        logging.warning("{}; the local parts will still be detached", message)
    return tables


def bind_parts_to_tables(
    parts: List[QuarantinedPart], tables: Dict[str, OfflineTable]
) -> None:
    for part in parts:
        part.table = tables.get(part.table_uuid)


def preflight_local_moves(
    parts: Iterable[QuarantinedPart], keep_going: bool = False
) -> List[QuarantinedPart]:
    """Resolve local destinations before the first mutation."""
    parts_by_key: Dict[tuple[str, str], List[QuarantinedPart]] = {}
    for part in parts:
        key = (part.table_uuid, part.part_name)
        parts_by_key.setdefault(key, []).append(part)

    result = []
    reserved_paths: Set[Path] = set()
    for (table_uuid, part_name), copies in parts_by_key.items():
        active_copies = [part for part in copies if not part.already_detached]
        detached_copies = [part for part in copies if part.already_detached]
        if not active_copies:
            result.append(detached_copies[0])
            continue

        part = active_copies[0]
        detached_dir = part.detached_path.parent
        if not detached_dir.is_dir() or detached_dir.is_symlink():
            raise ValueError(
                "Detached directory does not exist or is not a real directory: "
                f"{detached_dir}"
            )
        if not part.source_path.is_dir() or part.source_path.is_symlink():
            raise ValueError(f"Source part directory disappeared: {part.source_path}")

        if detached_copies:
            if not keep_going:
                raise ValueError(
                    f"Both active and detached copies exist for "
                    f"{table_uuid}/{part_name}"
                )
            conflict = (
                f"Both active and detached copies exist for {table_uuid}/{part_name}"
            )
        elif os.path.lexists(part.detached_path):
            if not keep_going:
                raise ValueError(f"Destination already exists: {part.detached_path}")
            conflict = f"Destination already exists: {part.detached_path}"
        else:
            conflict = None

        if conflict is not None:
            original_destination = part.detached_path
            retry = 1
            while True:
                retry_destination = Path(f"{original_destination}_try{retry}")
                if retry_destination not in reserved_paths and not os.path.lexists(
                    retry_destination
                ):
                    break
                retry += 1
            part.detached_path = retry_destination
            logging.warning(
                "{}; using {} for the active copy",
                conflict,
                retry_destination,
            )

        reserved_paths.add(part.detached_path)
        result.append(part)

    return result


def _tables_by_zookeeper_path(
    tables: Dict[str, OfflineTable],
) -> Dict[str, List[OfflineTable]]:
    result: Dict[str, List[OfflineTable]] = {}
    for table in tables.values():
        if table.zookeeper_path is not None:
            result.setdefault(table.zookeeper_path, []).append(table)
    return result


def ensure_nonlocal_zookeeper_replicas_inactive(
    zk: KazooClient,
    tables: Dict[str, OfflineTable],
    keep_going: bool = False,
) -> None:
    """Fail if a non-local replica of a selected table is active in Keeper.

    The local server is checked via ``ensure_clickhouse_server_stopped``.
    Its ``is_active`` znode can outlive server shutdown briefly, so it must not
    be used to decide whether this offline operation is safe.
    """
    for zookeeper_path, root_tables in _tables_by_zookeeper_path(tables).items():
        table_names = ", ".join(sorted(table.display_name for table in root_tables))
        replicas_path = posixpath.join(zookeeper_path, "replicas")
        if zk.exists(replicas_path) is None:
            message = (
                f"Keeper replicas path does not exist for {table_names}: "
                f"{replicas_path}"
            )
            if not keep_going:
                raise ValueError(message)
            logging.warning("{}; continuing in keep-going mode", message)
            continue

        local_replicas = {
            table.replica_name
            for table in root_tables
            if table.replica_name is not None
        }
        for replica in sorted(get_children(zk, replicas_path)):
            if replica in local_replicas:
                continue
            active_path = posixpath.join(replicas_path, replica, "is_active")
            if zk.exists(active_path) is not None:
                raise RuntimeError(
                    f"Keeper replica is active for {table_names}: "
                    f"{replica} ({active_path})"
                )


def _get_zookeeper_node_text(zk: KazooClient, path: str) -> str:
    value, _ = zk.get(path)
    return value.decode() if isinstance(value, bytes) else str(value)


def plan_replicas_to_mark_lost(
    zk: KazooClient,
    tables: Dict[str, OfflineTable],
    mark_replicas_lost: bool,
) -> ReplicaLostPlan:
    """Resolve local replicas and validate all is_lost targets before mutation."""
    local_replicas: Set[str] = set()
    replicas_to_mark_lost: Set[str] = set()
    nodes_to_mark_lost: Set[str] = set()

    for zookeeper_path, root_tables in _tables_by_zookeeper_path(tables).items():
        table_names = ", ".join(sorted(table.display_name for table in root_tables))
        replicas_path = posixpath.join(zookeeper_path, "replicas")
        if zk.exists(replicas_path) is None:
            message = f"Keeper replicas path does not exist: {replicas_path}"
            if mark_replicas_lost:
                raise _replica_identity_error(table_names, message)
            logging.warning(
                "Cannot verify the local replica for {}: {}; "
                "--no-mark-replicas-lost is active",
                table_names,
                message,
            )
            continue

        replicas = set(get_children(zk, replicas_path))
        root_local_replicas: Set[str] = set()
        for table in root_tables:
            if table.replica_name is None:
                if mark_replicas_lost:
                    raise _replica_identity_error(
                        table.display_name, "the replica name could not be resolved"
                    )
                continue
            if table.replica_name not in replicas:
                message = (
                    f"resolved name {table.replica_name!r} is absent under "
                    f"{replicas_path}; registered replicas: {sorted(replicas)}"
                )
                if mark_replicas_lost:
                    raise _replica_identity_error(table.display_name, message)
                logging.warning(
                    "Cannot verify the local replica for {}: {}; "
                    "--no-mark-replicas-lost is active",
                    table.display_name,
                    message,
                )
                continue
            root_local_replicas.add(table.replica_name)

        local_replicas.update(root_local_replicas)
        if not mark_replicas_lost:
            continue

        for local_replica in root_local_replicas:
            local_is_lost_path = posixpath.join(replicas_path, local_replica, "is_lost")
            if zk.exists(local_is_lost_path) is None:
                raise _replica_identity_error(
                    table_names,
                    f"local node does not exist: {local_is_lost_path}",
                )
            local_is_lost = _get_zookeeper_node_text(zk, local_is_lost_path)
            if local_is_lost != "0":
                raise _replica_identity_error(
                    table_names,
                    f"local node {local_is_lost_path} contains "
                    f"{local_is_lost!r}, not '0'",
                )

        for replica in replicas - root_local_replicas:
            is_lost_path = posixpath.join(replicas_path, replica, "is_lost")
            if zk.exists(is_lost_path) is None:
                raise ValueError(
                    f"Cannot mark replica {replica!r} lost for {table_names}: "
                    f"node does not exist: {is_lost_path}. Rerun with "
                    "--no-mark-replicas-lost to skip lost-replica marking"
                )
            replicas_to_mark_lost.add(replica)
            nodes_to_mark_lost.add(is_lost_path)

    return ReplicaLostPlan(
        local_replicas=sorted(local_replicas),
        replicas_to_mark_lost=sorted(replicas_to_mark_lost),
        nodes_to_mark_lost=sorted(nodes_to_mark_lost),
    )


def mark_zookeeper_replicas_lost(zk: KazooClient, nodes: Iterable[str]) -> int:
    """Idempotently put selected replicas into repair mode."""
    nodes = list(dict.fromkeys(nodes))
    for node in nodes:
        zk.set(node, b"1")
    return len(nodes)


def collect_zookeeper_part_nodes(
    zk: KazooClient,
    parts: List[QuarantinedPart],
    keep_going: bool = False,
) -> List[str]:
    """Resolve exact part znodes under every registered table replica."""
    result: List[str] = []
    seen_nodes: Set[str] = set()
    parts_by_root: Dict[str, List[QuarantinedPart]] = {}
    for part in parts:
        part.zookeeper_nodes.clear()
        if part.table is None or part.temporary or part.table.zookeeper_path is None:
            continue
        parts_by_root.setdefault(part.table.zookeeper_path, []).append(part)

    for zookeeper_path, root_parts in parts_by_root.items():
        table_names = sorted(
            {part.table.display_name for part in root_parts if part.table is not None}
        )
        replicas_path = posixpath.join(zookeeper_path, "replicas")
        if zk.exists(replicas_path) is None:
            message = (
                f"Keeper replicas path does not exist for {', '.join(table_names)}: "
                f"{replicas_path}"
            )
            if not keep_going:
                raise ValueError(message)
            logging.warning("{}; the local parts will still be detached", message)
            continue

        by_name: Dict[str, List[QuarantinedPart]] = {}
        for part in root_parts:
            by_name.setdefault(part.part_name, []).append(part)
        selected_names = set(by_name)
        found_names: Set[str] = set()
        for replica in sorted(get_children(zk, replicas_path)):
            replica_parts_path = posixpath.join(replicas_path, replica, "parts")
            if zk.exists(replica_parts_path) is None:
                message = f"Keeper parts path does not exist for replica {replica}: {replica_parts_path}"
                if not keep_going:
                    raise ValueError(message)
                logging.warning("{}; continuing in keep-going mode", message)
                continue
            replica_part_names = set(get_children(zk, replica_parts_path))
            for part_name in sorted(replica_part_names & selected_names):
                node = posixpath.join(replica_parts_path, part_name)
                for part in by_name[part_name]:
                    part.zookeeper_nodes.append(node)
                if node not in seen_nodes:
                    seen_nodes.add(node)
                    result.append(node)
                found_names.add(part_name)

        missing_names = sorted(selected_names - found_names)
        if missing_names:
            example = ", ".join(missing_names[:10])
            if len(missing_names) > 10:
                example += f", ... ({len(missing_names) - 10} more)"
            logging.warning(
                "Keeper has no exact /parts node on any replica for {} local "
                "part name(s) of {}: {}. The local parts will still be detached",
                len(missing_names),
                ", ".join(table_names),
                example,
            )
    return result


def move_parts_to_detached(parts: Iterable[QuarantinedPart]) -> int:
    moved = 0
    for part in parts:
        if part.already_detached:
            continue
        part.source_path.rename(part.detached_path)
        logging.info("Moved {} -> {}", part.source_path, part.detached_path)
        moved += 1
    return moved


def delete_zookeeper_part_nodes(zk: KazooClient, nodes: Iterable[str]) -> int:
    """Delete current and legacy part-znode layouts idempotently."""
    nodes = list(nodes)
    delete_nodes_batched(zk, nodes)
    return len(nodes)


def _manifest_payload(
    parts: List[QuarantinedPart],
    dry_run: bool,
    lost_plan: Optional[ReplicaLostPlan] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "hostname": socket.gethostname(),
        "dry_run": dry_run,
        "local_replicas": lost_plan.local_replicas if lost_plan else [],
        "replicas_to_mark_lost": (lost_plan.replicas_to_mark_lost if lost_plan else []),
        "parts": [],
    }
    for part in parts:
        payload["parts"].append(
            {
                "database": part.table.database if part.table else None,
                "table": part.table.table if part.table else None,
                "table_uuid": part.table_uuid,
                "part_name": part.part_name,
                "detached_path": str(part.detached_path),
                "zookeeper_nodes": list(part.zookeeper_nodes),
            }
        )
    return payload


def _write_new_manifest(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as manifest:
        json.dump(payload, manifest, indent=2, sort_keys=True)
        manifest.write("\n")
        manifest.flush()
        os.fsync(manifest.fileno())


def quarantine_object_storage_parts(
    ctx: Context,
    root_path: str,
    metadata_path: str,
    parts_file: Optional[str],
    dry_run: bool,
    detached_prefix: str = DETACHED_PREFIX,
    keep_going: bool = False,
    mark_replicas_lost: bool = True,
) -> QuarantineReport:
    """Move object-storage parts offline and remove their replica part znodes."""
    root = Path(root_path)
    metadata = Path(metadata_path)
    manifest_path = Path(parts_file) if parts_file else None

    if manifest_path is not None and os.path.lexists(manifest_path):
        raise ValueError(f"Parts file already exists: {manifest_path}")

    ensure_clickhouse_server_stopped()
    ensure_broken_detached_parts_are_not_cleaned(ctx)

    parts = discover_object_storage_parts(root, detached_prefix)
    if not parts:
        if manifest_path is not None:
            _write_new_manifest(manifest_path, _manifest_payload(parts, dry_run))
        return QuarantineReport([], 0, 0, 0, dry_run)

    clickhouse_config = get_clickhouse_config(ctx)
    tables = load_offline_tables(
        metadata,
        {part.table_uuid for part in parts},
        get_macros(ctx),
        default_replica_path=clickhouse_config.default_replica_path,
        default_replica_name=clickhouse_config.default_replica_name,
        require_replica_name=mark_replicas_lost,
        keep_going=keep_going,
    )
    bind_parts_to_tables(parts, tables)
    parts = preflight_local_moves(parts, keep_going)
    selected_table_uuids = {part.table_uuid for part in parts}
    tables = {
        table_uuid: table
        for table_uuid, table in tables.items()
        if table_uuid in selected_table_uuids
    }

    replicated_table_uuids = {
        part.table.uuid
        for part in parts
        if part.table is not None
        and part.table.zookeeper_path is not None
        and not part.temporary
    }
    keeper_tables = {
        table_uuid: table
        for table_uuid, table in tables.items()
        if table_uuid in replicated_table_uuids
    }

    def execute(zk: Optional[KazooClient]) -> QuarantineReport:
        ensure_clickhouse_server_stopped()
        if zk is not None:
            ensure_nonlocal_zookeeper_replicas_inactive(zk, keeper_tables, keep_going)
            nodes = collect_zookeeper_part_nodes(zk, parts, keep_going)
            lost_plan = plan_replicas_to_mark_lost(
                zk, keeper_tables, mark_replicas_lost
            )
        else:
            nodes = []
            lost_plan = ReplicaLostPlan([], [], [])

        logging.warning("Local replicas: {}", lost_plan.local_replicas)
        logging.warning("Replicas to mark lost: {}", lost_plan.replicas_to_mark_lost)

        if dry_run:
            for part in parts:
                if not part.already_detached:
                    logging.info(
                        "Will move {} -> {}", part.source_path, part.detached_path
                    )
            report = QuarantineReport(
                parts,
                moved=0,
                already_detached=sum(part.already_detached for part in parts),
                zookeeper_nodes=len(nodes),
                dry_run=True,
                local_replicas=lost_plan.local_replicas,
                replicas_to_mark_lost=lost_plan.replicas_to_mark_lost,
            )
            if manifest_path is not None:
                _write_new_manifest(
                    manifest_path, _manifest_payload(parts, dry_run, lost_plan)
                )
            return report

        moved = move_parts_to_detached(parts)
        if zk is not None:
            # Close the race where a server starts while local directories are
            # being renamed, before modifying Keeper state.
            ensure_clickhouse_server_stopped()
            ensure_nonlocal_zookeeper_replicas_inactive(zk, keeper_tables, keep_going)
            if mark_replicas_lost:
                current_lost_plan = plan_replicas_to_mark_lost(
                    zk, keeper_tables, mark_replicas_lost=True
                )
                if current_lost_plan != lost_plan:
                    raise RuntimeError(
                        "Keeper replica set changed while local parts were being "
                        "moved; no Keeper nodes were modified"
                    )
            delete_zookeeper_part_nodes(zk, nodes)
            if mark_replicas_lost:
                mark_zookeeper_replicas_lost(zk, lost_plan.nodes_to_mark_lost)

        report = QuarantineReport(
            parts,
            moved=moved,
            already_detached=sum(part.already_detached for part in parts),
            zookeeper_nodes=len(nodes),
            dry_run=False,
            local_replicas=lost_plan.local_replicas,
            replicas_to_mark_lost=lost_plan.replicas_to_mark_lost,
        )
        if manifest_path is not None:
            _write_new_manifest(
                manifest_path, _manifest_payload(parts, dry_run, lost_plan)
            )
        return report

    if replicated_table_uuids:
        with zk_client(ctx) as zk:
            return execute(zk)
    return execute(None)
