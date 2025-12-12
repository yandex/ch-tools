import os
import re
from pathlib import Path
from typing import (
    Generator,
    Optional,
)

from click import Context
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError, RolledBackError

from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    S3ObjectLocalMetaData,
)
from ch_tools.chadmin.internal.part import list_parts
from ch_tools.chadmin.internal.table_info import TableInfo
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.clickhouse.client.retry import retry
from ch_tools.common.clickhouse.config import get_clickhouse_config
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration
from ch_tools.common.process_pool import WorkerTask


def generate_zero_copy_lock_tasks(
    ctx: Context,
    disk: str,
    table: TableInfo,
    partition_id: Optional[str],
    part_name: Optional[str],
    replica: str,
    zookeeper_path: str,
    zk: KazooClient,
    dry_run: bool = False,
    zero_copy_path: Optional[str] = None,
    zero_copy_path_old: Optional[str] = None,
    copy_values: bool = False,
) -> Generator[WorkerTask, None, None]:
    """Generate tasks for creating zero-copy locks for given table/replica."""
    storage_config = S3DiskConfiguration.from_config(
        get_clickhouse_config(ctx).storage_configuration,
        disk,
        ctx.obj["config"]["object_storage"]["bucket_name_prefix"],
    )

    zero_copy_path = _get_zero_copy_zookeeper_path_for_disk_type(
        ctx, storage_config.OBJECT_STORAGE_TYPE, zero_copy_path, table["uuid"]
    )
    zero_copy_path_old = _get_zero_copy_zookeeper_path_for_disk_type(
        ctx, storage_config.OBJECT_STORAGE_TYPE, zero_copy_path_old, table["uuid"]
    )
    should_copy_lock_values = copy_values and zero_copy_path != zero_copy_path_old

    part_info = list_parts(
        ctx,
        database=table["database"],
        table=table["name"],
        disk_name=disk,
        part_name=part_name,
        partition_id=partition_id,
    )

    for part in part_info:
        lock_path, lock_path_old = _get_zero_copy_lock_path(
            storage_config.prefix,
            zero_copy_path,
            zero_copy_path_old,
            table["uuid"],
            part,
            replica,
        )

        part_path = _get_part_path_in_zk(part["name"], zookeeper_path, replica)

        task_id = f"{table['database']}.{table['name']}.{replica}.{part['name']}"
        yield WorkerTask(
            identifier=task_id,
            function=_create_single_zero_copy_lock,
            kwargs={
                "zk": zk,
                "lock_path": lock_path,
                "lock_path_old": lock_path_old,
                "part_path": part_path,
                "dry_run": dry_run,
                "copy_values": should_copy_lock_values,
            },
        )


def _get_first_checksums_blob_path(object_storage_prefix: str, part: dict) -> str:
    checksums_path = os.path.join(part["path"], "checksums.txt")
    metadata = S3ObjectLocalMetaData.from_file(Path(checksums_path))

    if metadata.has_full_object_key():
        if not metadata.objects[0].key.startswith(object_storage_prefix):
            raise RuntimeError(
                "Metadata file contains object storage prefix which is different from the one in storage config."
            )
        return metadata.objects[0].key

    return os.path.join(object_storage_prefix, metadata.objects[0].key.lstrip("/"))


def _get_zero_copy_lock_path(
    object_storage_prefix: str,
    zero_copy_path: str,
    zero_copy_path_old: str,
    table_uuid: str,
    part: dict,
    replica: str,
) -> tuple[str, str]:
    blob_path = _get_first_checksums_blob_path(object_storage_prefix, part)
    object_storage_path = blob_path.replace("/", "_")

    return os.path.join(
        zero_copy_path, table_uuid, part["name"], object_storage_path, replica
    ), os.path.join(
        zero_copy_path_old, table_uuid, part["name"], object_storage_path, replica
    )


def _get_part_path_in_zk(
    part: str,
    zookeeper_path: str,
    replica: str,
) -> str:
    return os.path.join(zookeeper_path, "replicas", replica, "parts", part)


def _get_zero_copy_zookeeper_path_for_disk_type(
    ctx: Context,
    disk_type: Optional[str] = "s3",
    zero_copy_path: Optional[str] = None,
    table_uuid: Optional[str] = None,
) -> str:
    """
    Returns ZooKeeper path for zero-copy table-independent info.

    '/clickhouse/zero_copy/zero_copy_s3' is default for s3 disk.
    """
    disk_dir = f"zero_copy_{disk_type}"

    if zero_copy_path:
        return os.path.join(zero_copy_path, disk_dir)

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
                f"Table with uuid {table_uuid} doesn't exist. Will search for locks in default 'remote_fs_zero_copy_zookeeper_path' directory."
            )

    query = "SELECT value FROM system.merge_tree_settings WHERE name = 'remote_fs_zero_copy_zookeeper_path'"
    base_path = execute_query(ctx, query, format_="JSONCompact")["data"][0][0]

    return os.path.join(base_path, disk_dir)


def _create_single_zero_copy_lock(
    zk: KazooClient,
    lock_path: str,
    lock_path_old: str,
    part_path: str,
    dry_run: bool = False,
    copy_values: bool = False,
) -> None:
    """Create a single zero-copy lock."""

    # Some parent path may be deleted or created concurrently
    @retry(
        exception_types=(NoNodeError, NodeExistsError), max_attempts=3, max_interval=1
    )
    def _create_lock_in_transaction(
        zk: KazooClient,
        lock_path: str,
        value: bytes,
        part_path: str,
        part_node_version: int,
    ) -> None:
        parents_to_create = _get_parent_paths_to_create(zk, lock_path)

        create_transaction = zk.transaction()
        create_transaction.check(part_path, part_node_version)

        for i, parent in enumerate(parents_to_create):
            if len(parents_to_create) >= 2 and i == len(parents_to_create) - 2:
                create_transaction.create(parent, value)
            else:
                create_transaction.create(parent)

        create_transaction.create(lock_path)
        results = create_transaction.commit()

        for result in results:
            if isinstance(result, Exception) and not isinstance(
                result, RolledBackError
            ):
                raise result

    if zk.exists(lock_path):
        return

    part_node = zk.exists(part_path)
    # This means part is already deleted
    if not part_node:
        return

    logging.info("Creating zero-copy lock at {}", lock_path)

    value = b""
    if copy_values and zk.exists(lock_path_old):
        # Value is stored in 'part' node, full path is '.../part/blob_path/replica'
        value_path = os.path.dirname(os.path.dirname(lock_path_old))
        try:
            value, _ = zk.get(value_path)
            logging.debug(
                "Found old lock at {}, will copy it's value from {}",
                lock_path_old,
                value_path,
            )
        except NoNodeError:
            # Lock was concurrently removed
            pass

    if dry_run:
        return

    try:
        _create_lock_in_transaction(zk, lock_path, value, part_path, part_node.version)
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
