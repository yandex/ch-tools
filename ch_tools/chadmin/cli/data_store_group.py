import os
import shutil
import subprocess
from typing import Any, Dict, List, Optional

import boto3
from click import Context, group, option, pass_context
from cloup.constraints import AcceptAtMost, constraint

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.clickhouse_disks import (
    CLICKHOUSE_METADATA_PATH,
    CLICKHOUSE_PATH,
    CLICKHOUSE_STORE_PATH,
    S3_METADATA_STORE_PATH,
    make_ch_disks_config,
    remove_from_ch_disk,
)
from ch_tools.chadmin.internal.object_storage.broken_partitions_recovery import (
    detach_broken_partition,
    find_broken_parts,
    group_broken_parts_by_partition,
    make_partition_report,
    restore_recoverable_broken_partitions,
)
from ch_tools.chadmin.internal.object_storage.parts_quarantine import (
    DETACHED_PREFIX,
    quarantine_object_storage_parts,
)
from ch_tools.chadmin.internal.system import get_version
from ch_tools.chadmin.internal.utils import remove_from_disk
from ch_tools.common import logging
from ch_tools.common.cli.formatting import print_response
from ch_tools.common.clickhouse.config import get_clickhouse_config
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration
from ch_tools.common.process_pool import WorkerTask, execute_tasks_in_parallel


@group("data-store", cls=Chadmin)
def data_store_group() -> None:
    """
    Commands for manipulating data stored by ClickHouse.
    """
    pass


def _object_storage_store_path(data_path: str, disk_name: str) -> str:
    """Build a store path for an existing ClickHouse disk directory."""
    disks_path = os.path.join(data_path, "disks")
    try:
        with os.scandir(disks_path) as entries:
            disk_directories = {
                entry.name: entry.path
                for entry in entries
                if entry.is_dir(follow_symlinks=False)
            }
    except OSError as error:
        raise ValueError(
            f"Cannot list ClickHouse disks directory {disks_path}: {error}"
        ) from error

    disk_path = disk_directories.get(disk_name)
    if disk_path is None:
        available = ", ".join(sorted(disk_directories)) or "none"
        raise ValueError(
            f"ClickHouse disk {disk_name!r} is not a directory under "
            f"{disks_path}; available disks: {available}"
        )
    return os.path.join(disk_path, "store")


@data_store_group.command("detach-object-storage-parts")
@option(
    "--disk",
    "disk_name",
    default="object_storage",
    show_default=True,
    help="Name of the object-storage disk.",
)
@option(
    "--detached-prefix",
    default=DETACHED_PREFIX,
    show_default=True,
    help="Prefix added to part directory names under detached/.",
)
@option(
    "--parts-file",
    default=None,
    help="Save the part list and Keeper paths to a new JSON file.",
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show the planned local moves and Keeper deletions without changing them.",
)
@option(
    "-k",
    "--keep-going",
    is_flag=True,
    default=False,
    help=(
        "Detach local parts even when metadata or Keeper paths are missing, "
        "or detached destination names conflict."
    ),
)
@option(
    "--mark-replicas-lost/--no-mark-replicas-lost",
    default=True,
    help=(
        "Mark every non-local replica as lost so it mimics this replica on its "
        "next start. Use --no-mark-replicas-lost when the local replica name "
        "cannot be determined safely."
    ),
)
@option("-v", "--verbose", is_flag=True, help="Print every selected part.")
@pass_context
def detach_object_storage_parts_command(
    ctx: Context,
    disk_name: str,
    detached_prefix: str,
    parts_file: Optional[str],
    dry_run: bool,
    keep_going: bool,
    mark_replicas_lost: bool,
    verbose: bool,
) -> None:
    """Offline-detach object-storage parts and remove them from replica /parts."""
    data_path = get_clickhouse_config(ctx).data_path
    root_path = _object_storage_store_path(data_path, disk_name)
    report = quarantine_object_storage_parts(
        ctx=ctx,
        root_path=root_path,
        metadata_path=os.path.join(data_path, "metadata"),
        parts_file=parts_file,
        dry_run=dry_run,
        detached_prefix=detached_prefix,
        keep_going=keep_going,
        mark_replicas_lost=mark_replicas_lost,
    )
    if verbose:
        print_response(ctx, [report.summary()], default_format="table")
    print_response(
        ctx,
        report.details() if verbose else [report.summary()],
        default_format="table",
    )


@data_store_group.command("clean-orphaned-tables")
@pass_context
@option(
    "--column",
    "column",
    default=None,
    help="Additional check: specified COLUMN name should exists in data to be removed. Example: `initial_query_start_time_microseconds.bin` for `query_log`-table.",
)
@option(
    "--remove",
    is_flag=True,
    default=False,
    help="Flag to REMOVE data from store subdirectories.",
)
@option(
    "--store-path",
    "store_path",
    default=CLICKHOUSE_STORE_PATH,
    help="Set the store subdirectory path.",
)
@option(
    "--show-all-metadata",
    "show_only_orphaned_metadata",
    is_flag=True,
    default=True,
    help="Flag to only orphaned metadata.",
)
def clean_orphaned_tables_command(
    ctx: Context,
    column: Optional[str],
    remove: bool,
    store_path: str,
    show_only_orphaned_metadata: bool,
) -> None:
    results: List[Dict[str, Any]] = []
    for prefix in os.listdir(store_path):
        path = store_path + "/" + prefix
        try:
            path_result = process_path(path, prefix, column, remove)
        except subprocess.CalledProcessError as e:
            if "No such file or directory" in e.stdout.decode("utf-8"):
                print("Skip directory {} because it is removed: {}", path, e.stdout)
                continue
            raise

        if show_only_orphaned_metadata and path_result["status"] != "not_used":
            continue
        results.append(path_result)

    print_response(ctx, results, default_format="table")


def process_path(
    path: str,
    prefix: str,
    column: Optional[str],
    remove: bool,
) -> Dict[str, Any]:
    logging.info("Processing path {} with prefix {}:", path, prefix)

    result: Dict[str, Any] = {
        "path": path,
        "status": "unknown",
        "size": 0,
        "removed": False,
    }

    size = du(path)
    logging.info("Size of path {}: {}", path, size)
    result["size"] = size

    file = prefix_exists_in_metadata(prefix)

    if file:
        logging.info('Prefix "{}" is used in metadata file "{}"', prefix, file)
        result["status"] = "used"
        return result

    if column and not additional_check_successed(column, path):
        logging.info("Additional check for column-parameter not passed")
        result["status"] = "not_passed_column_check"
        return result

    logging.info('Prefix "{}" is NOT used in any metadata file', prefix)
    result["status"] = "not_used"

    if remove:
        logging.info('Trying to remove path "{}"', path)

        remove_data(path)
        result["removed"] = True
    else:
        logging.info(
            'Path "{}" is not removed because of remove-parameter is not specified',
            path,
        )
        result["removed"] = False

    return result


def prefix_exists_in_metadata(prefix: str) -> Optional[str]:
    for w in os.walk(CLICKHOUSE_PATH):
        dir_name = w[0]
        filenames = w[2]

        for file in filenames:
            if not file.endswith(".sql"):
                continue

            with open(dir_name + "/" + file, encoding="utf-8") as f:
                if f"'{prefix}" in f.read():
                    return file

    return None


def additional_check_successed(column: str, path: str) -> bool:
    for w in os.walk(path):
        filenames = w[2]

        columns = [file for file in filenames if column in file]
        if columns:
            return True

    return False


def du(path: str) -> str:
    return (
        subprocess.check_output(["du", "-sh", path], stderr=subprocess.STDOUT)
        .split()[0]
        .decode("utf-8")
    )


def remove_data(path: str) -> None:
    def onerror(*args: Any) -> None:
        errors = "\n".join(list(args))
        logging.error("ERROR: {}", errors)

    shutil.rmtree(path=path, onerror=onerror)  # pylint: disable=deprecated-argument


@data_store_group.command("cleanup-data-dir")
@pass_context
@option(
    "--remove",
    is_flag=True,
    default=False,
    help="Flag to REMOVE data from store subdirectories.",
)
@option(
    "--disk",
    "disk",
    default="default",
    help="Set the data subdirectory path.",
)
@option(
    "--keep-going",
    is_flag=True,
    default=False,
    help="Flag to REMOVE data from store subdirectories.",
)
@option(
    "--max-sql-objects",
    default=1000000,
    help="Restriction for max count of sql objects.",
)
@option(
    "--max-workers",
    default=4,
    help="Max workers for removing.",
)
@option(
    "--remove-only-metadata",
    is_flag=True,
    default=False,
    help="Flag to remove only local metadata.",
)
def cleanup_data_dir(
    ctx: Context,
    remove: bool,
    disk: str,
    keep_going: bool,
    max_sql_objects: int,
    max_workers: int,
    remove_only_metadata: bool,
) -> None:
    lost_data: List[Dict[str, Any]] = []
    path_to_disk = CLICKHOUSE_PATH + (f"/disks/{disk}" if disk != "default" else "")
    data_path = path_to_disk + "/data"

    collect_orphaned_sql_objects_recursive(
        CLICKHOUSE_METADATA_PATH,
        data_path,
        lost_data,
        0,
        1,
        max_sql_objects,
    )

    if remove:
        disk_config_path = make_ch_disks_config(disk)

        tasks: List[WorkerTask] = []
        if remove_only_metadata:
            for data in lost_data:
                task = WorkerTask(
                    data["path"], remove_orphaned_sql_object_metadata, {"data": data}
                )
                tasks.append(task)
        else:
            for data in lost_data:
                task = WorkerTask(
                    data["path"],
                    remove_orphaned_sql_object_full,
                    {
                        "data": data,
                        "disk": disk,
                        "path_to_disk": path_to_disk,
                        "disks_config_path": disk_config_path,
                        "ch_version": get_version(ctx),
                    },
                )
                tasks.append(task)
        execute_tasks_in_parallel(tasks, max_workers=max_workers, keep_going=keep_going)
    print_response(ctx, lost_data, default_format="table")


def remove_orphaned_sql_object_metadata(data: Dict[str, Any]) -> None:
    path = data["path"]

    remove_from_disk(path)
    data["deleted"] = "Yes"


def remove_orphaned_sql_object_full(
    data: Dict[str, Any],
    disk: str,
    path_to_disk: str,
    ch_version: str,
    disks_config_path: str,
) -> None:
    path = data["path"]

    if not path.startswith(path_to_disk):
        raise RuntimeError(f"Path {path} on fs does not math with disk {disk}")
    relative_path_on_disk = path[len(path_to_disk) + 1 :]
    retcode, stderr = remove_from_ch_disk(
        disk=disk,
        path=relative_path_on_disk,
        ch_version=ch_version,
        disk_config_path=disks_config_path,
    )
    if retcode:
        raise RuntimeError(
            f"clickhouse-disks remove command has failed: retcode {retcode}, stderr: {stderr.decode()}"
        )

    data["deleted"] = "Yes"


def collect_orphaned_sql_objects_recursive(
    metadata_path: str,
    data_path: str,
    lost_data: List[Dict[str, Any]],
    depth: int,
    max_depth: int,
    max_sql_objects: int,
) -> None:
    sql_suff = ".sql"
    # Extract all active sql object from metadata dir
    if max_sql_objects == len(lost_data):
        return

    list_sql_objects = [
        entry.name[: -len(sql_suff)]
        for entry in os.scandir(metadata_path)
        if entry.is_file() and entry.name.endswith(sql_suff)
    ]

    for entry in os.scandir(data_path):
        if max_sql_objects == len(lost_data):
            return

        if not entry.is_dir():
            continue
        if entry.name not in list_sql_objects:
            lost_data.append({"path": entry.path, "deleted": "No"})
            continue
        if max_depth >= depth + 1:
            collect_orphaned_sql_objects_recursive(
                metadata_path + "/" + entry.name,
                entry.path,
                lost_data,
                depth + 1,
                max_depth,
                max_sql_objects,
            )


@data_store_group.command("detect-broken-partitions")
@option(
    "--root-path",
    "root_path",
    default=S3_METADATA_STORE_PATH,
    help="Set the store subdirectory path.",
)
@option(
    "--reattach",
    is_flag=True,
    default=False,
    help="Flag to reattach broken partitions.",
)
@option(
    "--detach",
    is_flag=True,
    default=False,
    help="Flag to detach broken partitions.",
)
@option(
    "--restore-recoverable",
    is_flag=True,
    default=False,
    help="Flag to restore safely recoverable missing S3 objects.",
)
@constraint(AcceptAtMost(1), ["detach", "reattach"])
@pass_context
def detect_broken_partitions(
    ctx: Context,
    root_path: str,
    reattach: bool,
    detach: bool,
    restore_recoverable: bool,
) -> None:
    ch_config = get_clickhouse_config(ctx)

    disk_conf = S3DiskConfiguration.from_config(
        ch_config.storage_configuration,
        "object_storage",
        ctx.obj["config"]["object_storage"]["bucket_name_prefix"],
    )
    s3_client = boto3.client(
        "s3",
        endpoint_url=disk_conf.endpoint_url,
        aws_access_key_id=disk_conf.access_key_id,
        aws_secret_access_key=disk_conf.secret_access_key,
    )

    if restore_recoverable:
        restore_report = restore_recoverable_broken_partitions(
            ctx=ctx,
            root_path=root_path,
            s3_client=s3_client,
            disk_conf=disk_conf,
            detach_unrecoverable=detach,
            reattach_unrecoverable=reattach,
        )
        print_response(ctx, restore_report, default_format="table")
        return

    broken_parts = find_broken_parts(ctx, root_path, s3_client, disk_conf)
    broken_parts_by_partition = group_broken_parts_by_partition(broken_parts)
    repaired_partitions = []

    for partition_key in sorted(broken_parts_by_partition):
        partition_broken_parts = broken_parts_by_partition[partition_key]
        broken_part = partition_broken_parts[0]
        part_info = broken_part.info
        repaired_partitions.append(make_partition_report(part_info))

        logging.debug(
            "Found the partition with missing blobs in object storage: paths={} table={} partition={}",
            [item.path for item in partition_broken_parts],
            part_info.table_id,
            part_info.partition_id,
        )
        if detach:
            detach_broken_partition(ctx, part_info, reattach=False)
        elif reattach:
            detach_broken_partition(ctx, part_info)

    print_response(ctx, repaired_partitions, default_format="table")

    logging.debug(
        "Found parts with missing s3 keys. Local paths of parts: {}",
        [broken_part.path for broken_part in broken_parts],
    )
