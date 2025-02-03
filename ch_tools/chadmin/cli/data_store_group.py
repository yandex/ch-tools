import os
import shutil
import subprocess
from pathlib import Path
from typing import List, NamedTuple, Optional, Set, Tuple

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
from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    S3ObjectLocalInfo,
    S3ObjectLocalMetaData,
)
from ch_tools.chadmin.internal.system import get_version
from ch_tools.chadmin.internal.utils import execute_query, remove_from_disk
from ch_tools.common import logging
from ch_tools.common.cli.formatting import print_response
from ch_tools.common.clickhouse.client import OutputFormat
from ch_tools.common.clickhouse.config import get_clickhouse_config
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration
from ch_tools.common.process_pool import WorkerTask, execute_tasks_in_parallel

ATTACH_DETTACH_TIMEOUT = 5000
ATTACH_DETACH_QUERY_RETRY = 10


class TablePartition(NamedTuple):
    table: str
    partition: str


@group("data-store", cls=Chadmin)
def data_store_group():
    """
    Commands for manipulating data stored by ClickHouse.
    """
    pass


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
    ctx, column, remove, store_path, show_only_orphaned_metadata
):
    results = []
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
    column: str,
    remove: bool,
) -> dict:
    logging.info("Processing path {} with prefix {}:", path, prefix)

    result = {
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
    def onerror(*args):
        errors = "\n".join(list(args))
        logging.error("ERROR: {}", errors)

    shutil.rmtree(path=path, onerror=onerror)


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
    ctx, remove, disk, keep_going, max_sql_objects, max_workers, remove_only_metadata
):
    lost_data: List[dict] = []
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


def remove_orphaned_sql_object_metadata(data):
    path = data["path"]

    retcode, stderr = remove_from_disk(path)
    if retcode:
        raise RuntimeError(
            f"Metadata remove command has failed: retcode {retcode}, stderr: {stderr.decode()}"
        )
    data["deleted"] = "Yes"


def remove_orphaned_sql_object_full(
    data, disk, path_to_disk, ch_version, disks_config_path
):

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
    lost_data: list,
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
@constraint(AcceptAtMost(1), ["detach", "reattach"])
@pass_context
def detect_broken_partitions(ctx, root_path, reattach, detach):
    ch_config = get_clickhouse_config(ctx)

    disk_conf: S3DiskConfiguration = (
        ch_config.storage_configuration.s3_disk_configuration(
            "object_storage", ctx.obj["config"]["object_storage"]["bucket_name_prefix"]
        )
    )
    s3_client = boto3.client(
        "s3",
        endpoint_url=disk_conf.endpoint_url,
        aws_access_key_id=disk_conf.access_key_id,
        aws_secret_access_key=disk_conf.secret_access_key,
    )
    repaired_partitions = set()

    for path, _, files in os.walk(root_path):
        objects: List[S3ObjectLocalInfo] = []
        logging.debug(f"Checking files from: {path}")
        for file in files:
            try:
                file_full_path = Path(os.path.join(path, file))
                objects.extend(S3ObjectLocalMetaData.from_file(file_full_path).objects)
            except Exception as e:
                logging.error("Failed to perform extend objects: {!r}", e)

        for s3_object in objects:
            object_storage_key = os.path.join(disk_conf.prefix, s3_object.key)

            if check_key_in_object_storage(
                s3_client, disk_conf.bucket_name, object_storage_key
            ):
                continue

            logging.debug("Not found key {}", object_storage_key)

            table_partition = get_partition_by_path(ctx, path)

            if table_partition is None:
                logging.warning("Skip failed path {}.", path)
                break

            if table_partition not in repaired_partitions:
                repaired_partitions.add(table_partition)

                logging.debug(
                    "Found the partition with missing blob in the object storage: path={} table={} partition={} ",
                    path,
                    table_partition.table,
                    table_partition.partition,
                )
                if detach:
                    try_repair_partition(ctx, table_partition, False)
                elif reattach:
                    try_repair_partition(ctx, table_partition)
            else:
                logging.debug(
                    "Partition {} for table {} was already repared. Skip.",
                    table_partition.partition,
                    table_partition.table,
                )

            break

    print_partitions(ctx, repaired_partitions)

    logging.debug(
        "Found parts with missing s3 keys. Local paths of parts: {}",
        repaired_partitions,
    )


def try_repair_partition(
    ctx: Context, table_partition: TablePartition, attach: bool = True
) -> None:
    """
    Try to repair broken partition with DETACH and optional ATTACH.
    """
    detach_partition(ctx, table_partition)

    if attach:
        attach_partition(ctx, table_partition)


def check_key_in_object_storage(s3_client: boto3.client, bucket: str, key: str) -> bool:
    """
    Check that object exists in s3 bucket with the specified key.
    """
    s3_resp = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=key,
    )
    if "Contents" not in s3_resp:
        return False
    if len(s3_resp["Contents"]) != 1:
        return False
    res = s3_resp["Contents"][0]["Key"] == key
    return res


def get_partition_by_path(ctx: Context, path: str) -> Optional[TablePartition]:
    """
    Get partition from path
    """
    query_string = (
        f"SELECT database, table, partition FROM system.parts WHERE path='{path}/'"
    )
    res = execute_query(ctx, query_string, format_=OutputFormat.JSONCompact)
    if "data" not in res:
        logging.warning("Not found data for part with path {}", path)
        return None

    if len(res["data"]) != 1 or len(res["data"][0]) != 3:
        return None

    table = f"`{res['data'][0][0]}`.`{res['data'][0][1]}`"
    partition = res["data"][0][2]
    return TablePartition(table, partition)


def print_partitions(ctx: Context, repaired_partitions: Set[TablePartition]) -> None:
    """
    For each path of part match corresponding table and partition.
    """

    # It's not really necessary, just to make output stable for tests.
    partitions_list: List[Tuple[str, str]] = list(repaired_partitions)
    partitions_list.sort()

    result = [
        {"table": partition[0], "partition": partition[1]}
        for partition in partitions_list
    ]

    print_response(ctx, result, default_format="table")


def query_with_retry(ctx: Context, query: str, timeout: int, retries: int) -> bool:
    """
    Execute clickhouse query with given number of retries.
    """
    logging.debug("Execute query: {}", query)
    for retry in range(retries):
        try:
            res = execute_query(ctx, query, timeout=timeout)
            if res == "":
                break
        except Exception as e:
            if retry + 1 == retries:
                logging.warning("Query {} failed  with:  {!r}\n", query, e)
                return False
            continue

    logging.info("Query {} finished successfully", query)
    return True


def detach_partition(ctx: Context, table_partition: TablePartition) -> bool:
    """
    Run DETACH the partition.
    """

    detach_query = f"ALTER TABLE {table_partition.table} DETACH PARTITION '{table_partition.partition}'"
    return query_with_retry(
        ctx,
        detach_query,
        timeout=ATTACH_DETTACH_TIMEOUT,
        retries=ATTACH_DETACH_QUERY_RETRY,
    )


def attach_partition(ctx: Context, table_partition: TablePartition) -> bool:
    """
    Run ATTACH the partition.
    """

    attach_query = f"ALTER TABLE {table_partition.table} ATTACH PARTITION '{table_partition.partition}'"
    # To avoid keeping detached partitions, perform the attach query with double attempts.
    return query_with_retry(
        ctx,
        attach_query,
        timeout=ATTACH_DETTACH_TIMEOUT,
        retries=2 * ATTACH_DETACH_QUERY_RETRY,
    )
