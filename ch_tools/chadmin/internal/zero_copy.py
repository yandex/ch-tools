import os
import re
import traceback
from pathlib import Path
from typing import (
    Generator,
    Optional,
    TypedDict,
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


class ZeroCopyLockInfo(TypedDict):
    lock_path: str
    part_path: str
    replica: str


def generate_zero_copy_lock_tasks(
    ctx: Context,
    disk: str,
    table: TableInfo,
    partition_id: Optional[str],
    part_name: Optional[str],
    replicas_to_lock: list[str],
    table_zk_path: str,
    zk: KazooClient,
    dry_run: bool = False,
    zero_copy_path: Optional[str] = None,
    zero_copy_path_old: Optional[str] = None,
    copy_values: bool = False,
    check_part_exist: bool = True,
) -> Generator[WorkerTask, None, None]:
    """Generate tasks for creating zero-copy locks for given table/replicas."""
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
        lock_infos = []

        for replica in replicas_to_lock:
            lock_infos.append(
                _get_zero_copy_lock_info(
                    storage_config.prefix,
                    table_zk_path,
                    zero_copy_path,
                    table["uuid"],
                    part,
                    replica,
                )
            )

        # Part name segment of zero-copy lock keeps hardlinked files
        set_value_path = os.path.join(zero_copy_path, table["uuid"], part["name"])
        old_value_path = os.path.join(zero_copy_path_old, table["uuid"], part["name"])

        task_id = f"{table['database']}.{table['name']}.{part['name']}"
        yield WorkerTask(
            identifier=task_id,
            function=_create_single_zero_copy_lock,
            kwargs={
                "zk": zk,
                "dry_run": dry_run,
                "copy_values": should_copy_lock_values,
                "set_value_path": set_value_path,
                "old_value_path": old_value_path,
                "lock_infos": lock_infos,
                "check_part_exist": check_part_exist,
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


def _get_zero_copy_lock_info(
    object_storage_prefix: str,
    table_zk_path: str,
    zero_copy_path: str,
    table_uuid: str,
    part: dict,
    replica: str,
) -> ZeroCopyLockInfo:
    blob_path = _get_first_checksums_blob_path(object_storage_prefix, part)
    object_storage_path = blob_path.replace("/", "_")

    return ZeroCopyLockInfo(
        lock_path=os.path.join(
            zero_copy_path, table_uuid, part["name"], object_storage_path, replica
        ),
        part_path=_get_part_path_in_zk(part["name"], table_zk_path, replica),
        replica=replica,
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
    lock_infos: list[ZeroCopyLockInfo],
    set_value_path: str,
    old_value_path: str,
    dry_run: bool = False,
    copy_values: bool = False,
    check_part_exist: bool = True,
) -> None:
    """Create zero-copy locks for multiple replicas in a single transaction."""
    # pylint: disable=too-many-branches

    # Some parent path may be deleted or created concurrently
    @retry(
        exception_types=(NoNodeError, NodeExistsError), max_attempts=3, max_interval=1
    )
    def _create_locks_in_transaction(
        zk: KazooClient, lock_infos: list[ZeroCopyLockInfo]
    ) -> None:

        locks_to_create = []
        part_checks = []

        # Check existing locks and part paths
        for lock_info in lock_infos:
            if zk.exists(lock_info["lock_path"]):
                logging.debug(
                    f"Zero-copy lock path '{lock_info['lock_path']}' already exists. Skip it"
                )
                continue

            part_stat = None
            if check_part_exist:
                part_stat = zk.exists(lock_info["part_path"])
                if not part_stat:
                    logging.debug(
                        f"Part path '{lock_info['part_path']}' is already removed. Skip it"
                    )
                    continue
                part_checks.append((lock_info["part_path"], part_stat))
            locks_to_create.append(lock_info["lock_path"])

        if not locks_to_create:
            return

        value = b""
        if copy_values and zk.exists(old_value_path):
            try:
                logging.debug(
                    f"Will copy old zero-copy lock '{old_value_path}' value to {set_value_path}"
                )
                value, _ = zk.get(old_value_path)
            except NoNodeError:
                logging.debug(
                    f"Old zero-copy lock '{old_value_path}' was removed concurrently. Continue"
                )

        for lock_path in locks_to_create:
            logging.debug(f"Create zero-copy lock {lock_path} in transaction")

        if dry_run:
            return

        # Collect all parent paths that need to be created
        all_parents_to_create = set()
        for lock_path in locks_to_create:
            parents = _get_parent_paths_to_create(zk, lock_path)
            all_parents_to_create.update(parents)

        # Create transaction
        create_transaction = zk.transaction()

        # Check table part nodes
        for part_path, part_stat in part_checks:
            create_transaction.check(part_path, part_stat.version)

        # Create parent nodes
        for parent in sorted(all_parents_to_create):
            if parent.strip("/") == set_value_path.strip("/"):
                create_transaction.create(parent, value)
            else:
                create_transaction.create(parent)

        # Create lock nodes
        for lock_path in locks_to_create:
            create_transaction.create(lock_path)

        results = create_transaction.commit()

        for result in results:
            if isinstance(result, Exception) and not isinstance(
                result, RolledBackError
            ):
                raise result

    replicas = [info["replica"] for info in lock_infos]
    logging.info("Creating zero-copy locks for replicas: {}", ", ".join(replicas))

    try:
        _create_locks_in_transaction(zk, lock_infos)
    except Exception as e:
        raise RuntimeError(
            f"Failed to create zero-copy locks for replicas {replicas}, reason: {e}\n"
            f"Traceback: {traceback.format_exc()}"
        )

    logging.info("Created zero-copy locks for replicas: {}", ", ".join(replicas))


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
