import os
import re
from typing import (
    Any,
    Optional,
)

from click import Context
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, RolledBackError

from ch_tools.chadmin.internal.system import match_ch_version
from ch_tools.chadmin.internal.table_replica import get_table_replica
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.chadmin.internal.zookeeper import zk_client
from ch_tools.common import logging
from ch_tools.common.clickhouse.client.retry import retry

CREATE_ZERO_COPY_LOCKS_BATCH_SIZE = 1000


def create_zero_copy_locks(
    ctx: Context,
    disk: str,
    disk_type: Optional[str],
    table: dict[str, str],
    partition_id: Optional[str],
    part_name: Optional[str],
    replica: str,
    dry_run: bool = False,
) -> None:
    """Create missing zero-copy locks for given tables."""
    disk_info = _get_disk_info(ctx, disk, disk_type)

    zero_copy_path = _get_zero_copy_zookeeper_path(
        ctx, disk_info["object_storage_type"], table["uuid"]
    )

    part_info = execute_query(
        ctx,
        """SELECT name, path FROM system.parts
            WHERE disk_name = '{{disk}}' AND table = '{{table_name}}'
                AND database = '{{database_name}}'
                {% if partition -%}
                AND partition_id = '{{partition_id}}'
                {% endif -%}
                {% if part -%}
                AND name = '{{part_name}}'
                {% endif -%}
        """,
        format_="JSON",
        disk=disk,
        table_name=table["name"],
        database_name=table["database"],
        part_name=part_name,
        partition_id=partition_id,
    )["data"]

    zero_copy_lock_paths = []
    object_storage_prefix = None
    for part in part_info:
        if not object_storage_prefix:
            object_storage_prefix = _get_object_storage_prefix(ctx, part)

        zero_copy_lock_paths.append(
            (
                _get_zero_copy_lock_path(
                    object_storage_prefix, zero_copy_path, table["uuid"], part, replica
                ),
                _get_part_path_in_zk(
                    ctx, table["database"], table["name"], part["name"], replica
                ),
            )
        )
        if len(zero_copy_lock_paths) == CREATE_ZERO_COPY_LOCKS_BATCH_SIZE:
            _create_zero_copy_locks(ctx, zero_copy_lock_paths, dry_run)
            zero_copy_lock_paths.clear()

    if zero_copy_lock_paths:
        _create_zero_copy_locks(ctx, zero_copy_lock_paths, dry_run)


def _get_object_storage_prefix(ctx: Context, part: dict) -> str:
    checksums_path = os.path.join(part["path"], "checksums.txt")
    metadata = _read_metadata(checksums_path)
    blob_path = metadata["keys"][0]["key"]

    return execute_query(
        ctx,
        f"SELECT remote_path FROM system.remote_data_paths WHERE remote_path LIKE '%{blob_path}'",
        format_="JSON",
    )["data"][0]["remote_path"].removesuffix(blob_path)


def _get_zero_copy_lock_path(
    object_storage_prefix: str,
    zero_copy_path: str,
    table_uuid: str,
    part: dict,
    replica: str,
) -> str:
    checksums_path = os.path.join(part["path"], "checksums.txt")
    metadata = _read_metadata(checksums_path)
    blob_path = metadata["keys"][0]["key"]

    object_storage_path = os.path.join(object_storage_prefix, blob_path).replace(
        "/", "_"
    )

    return os.path.join(
        zero_copy_path, table_uuid, part["name"], object_storage_path, replica
    )


def _get_part_path_in_zk(
    ctx: Context,
    database: str,
    table: str,
    part: str,
    replica: str,
) -> str:
    replica_info = get_table_replica(ctx, database, table)
    return os.path.join(
        replica_info["zookeeper_path"], "replicas", replica, "parts", part
    )


def _get_zero_copy_zookeeper_path(
    ctx: Context, disk_type: Optional[str] = "s3", table_uuid: Optional[str] = None
) -> str:
    """
    Returns ZooKeeper path for zero-copy table-independent info.

    '/clickhouse/zero_copy/zero_copy_s3' is default for s3 disk.
    """
    disk_dir = f"zero_copy_{disk_type}"

    if table_uuid:
        get_settings_query = (
            f"SELECT engine_full FROM system.tables WHERE uuid = '{table_uuid}'"
        )
        settings = execute_query(ctx, get_settings_query, format_="JSONCompact")["data"]
        if settings:
            match = re.search(
                r"remote_fs_zero_copy_zookeeper_path\s*=\s*'([^']+)'", settings[0][0]
            )
            if match:
                return os.path.join(match[1], disk_dir)
        else:
            logging.warning(
                "Table with uuid {} doesn't exist. Will search for locks in default 'remote_fs_zero_copy_zookeeper_path' directory.",
                table_uuid,
            )

    query = "SELECT value FROM system.merge_tree_settings WHERE name = 'remote_fs_zero_copy_zookeeper_path'"
    base_path = execute_query(ctx, query, format_="JSONCompact")["data"][0][0]

    return os.path.join(base_path, disk_dir)


def _read_metadata(path: str) -> dict:
    res: dict[str, Any] = {}
    with open(path, encoding="utf-8") as f:
        res["version"] = int(f.readline().strip())
        if res["version"] < 1 or res["version"] > 5:
            raise RuntimeError(f"Unknown metadata version: {res['version']}")

        line = f.readline().strip().split("\t")
        res["keys_count"] = int(line[0])
        res["total_size"] = int(line[1])

        res["keys"] = []
        for _ in range(res["keys_count"]):
            line = f.readline().strip().split("\t")
            res["keys"].append({"size": int(line[0]), "key": line[1]})

    return res


def _create_zero_copy_locks(
    ctx: Context,
    paths: list[tuple[str, str]],
    dry_run: bool = False,
) -> None:
    @retry(NoNodeError, max_attempts=3, max_interval=1)
    def _create_lock_in_transaction(
        zk: KazooClient, lock_path: str, part_path: str, part_node_version: int
    ) -> None:
        parents_to_create = _get_parent_paths_to_create(zk, lock_path)

        create_transaction = zk.transaction()
        create_transaction.check(part_path, part_node_version)

        for parent in parents_to_create:
            create_transaction.create(parent)

        create_transaction.create(lock_path)
        results = create_transaction.commit()

        for result in results:
            if isinstance(result, Exception) and not isinstance(
                result, RolledBackError
            ):
                raise result

    with zk_client(ctx) as zk:
        for lock_path, part_path in paths:
            if zk.exists(lock_path):
                continue

            part_node = zk.exists(part_path)
            # This means part is already deleted
            if not part_node:
                continue

            logging.info("Creating zero-copy lock at {}", lock_path)
            if dry_run:
                continue

            try:
                _create_lock_in_transaction(zk, lock_path, part_path, part_node.version)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to create zero-copy lock at {lock_path}, reason: {e}"
                )

            logging.info("Created zero-copy lock at {}", lock_path)


def _get_parent_paths_to_create(zk: KazooClient, path: str) -> list[str]:
    parents = path.split("/")[:-1]
    parent_path = ""
    paths_to_create = []

    all_exist = True
    for parent in parents:
        parent_path = os.path.join(parent_path, parent)
        if all_exist and zk.exists(parent_path):
            continue

        all_exist = False
        paths_to_create.append(parent_path)

    return paths_to_create


def _get_disk_info(ctx: Context, disk: str, disk_type: Optional[str]) -> dict:
    have_type_column = match_ch_version(ctx, "24.3")

    columns = "path, object_storage_type" if have_type_column else "path"
    query = f"SELECT {columns} FROM system.disks WHERE name = '{disk}'"
    disk_info = execute_query(
        ctx,
        query,
        format_="JSON",
    )["data"]
    if not disk_info:
        raise RuntimeError(f"Disk {disk} doesn't exist")
    disk_info = disk_info[0]

    if disk_type:
        disk_info["object_storage_type"] = disk_type
    else:
        if not have_type_column:
            raise RuntimeError(
                "--disk-type option is required for current version of ClickHouse"
            )
        disk_info["object_storage_type"] = disk_info["object_storage_type"].lower()

    return disk_info
