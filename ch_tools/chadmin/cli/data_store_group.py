import os
import re
import shutil
import subprocess
from typing import List, Optional, Tuple

import boto3
from click import Context, group, option, pass_context

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.clickhouse_disks import (
    CLICKHOUSE_METADATA_PATH,
    CLICKHOUSE_PATH,
    CLICKHOUSE_STORE_PATH,
    S3_METADATA_STORE_PATH,
    make_ch_disks_config,
    remove_from_ch_disk,
)
from ch_tools.chadmin.internal.utils import execute_query, remove_from_disk
from ch_tools.common import logging
from ch_tools.common.cli.formatting import print_response
from ch_tools.common.clickhouse.client import OutputFormat
from ch_tools.common.clickhouse.config import get_clickhouse_config
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration
from ch_tools.common.process_pool import WorkerTask, execute_tasks_in_parallel


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


def remove_orphaned_sql_object_full(data, disk, path_to_disk, disks_config_path):

    path = data["path"]

    if not path.startswith(path_to_disk):
        raise RuntimeError(f"Path {path} on fs does not math with disk {disk}")
    relative_path_on_disk = path[len(path_to_disk) + 1 :]
    retcode, stderr = remove_from_ch_disk(
        disk, relative_path_on_disk, disks_config_path
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
@pass_context
def detect_broken_partitions(ctx, root_path, reattach):
    parts_paths_with_lost_keys = find_paths_to_part_with_lost_keys(ctx, root_path)
    partition_list = get_partition_list(ctx, parts_paths_with_lost_keys)

    print_response(ctx, partition_list, default_format="table")
    if reattach:
        for partition_info in partition_list:
            reattach_partition(
                ctx, partition_info["table"], partition_info["partition"]
            )


def find_paths_to_part_with_lost_keys(ctx: Context, root_path: str) -> List[str]:
    result = []

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

    for path, _, files in os.walk(root_path):
        keys = []
        logging.debug(f"Checking files from: {path}")
        for file in files:
            keys.extend(get_keys_from_file(os.path.join(path, file)))

        for key in keys:
            full_key = os.path.join(disk_conf.prefix, key)
            if not check_key_in_object_storage(
                s3_client, disk_conf.bucket_name, full_key
            ):
                logging.debug("Not found key {}", full_key)
                result.append(path)
                logging.debug(
                    "Add part to check with path", result[-1][0], result[-1][1]
                )
                break

    logging.debug("Found parts with missing s3 keys. Local paths of parts: {}", result)
    return result


def get_keys_from_file(path):
    keys = []
    key_entry_regexp = re.compile("[0-9]+	[a-z]+/[a-z]+$")
    try:
        with open(path, encoding="utf-8") as file:
            lines = file.readlines()

            if len(lines) != 5:
                return
            for line in lines:
                if key_entry_regexp.match(line):
                    keys.append(line.split()[1])
    except Exception as e:
        logging.warning("Can not read file {!r}\n", e)
    return keys


def check_key_in_object_storage(s3_client, bucket, key):
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


def get_partition_list(ctx, parts_paths):
    partitions = set()
    for path in parts_paths:
        query_string = (
            f"SELECT database, table, partition FROM system.parts WHERE path='{path}/'"
        )
        res = execute_query(ctx, query_string, format_=OutputFormat.JSONCompact)
        if "data" not in res:
            logging.warning("Not found data for part with path {}", path)
            continue

        if len(res["data"]) != 1 or len(res["data"][0]) != 3:
            continue
        table = f"`{res['data'][0][0]}`.`{res['data'][0][1]}`"
        partition = res["data"][0][2]
        partitions.add((table, partition))

    # It's not really necessary, just to make output stable for tests.
    partitions_list: List[Tuple[str, str]] = list(partitions)
    partitions_list.sort()

    result = [
        {"table": partition[0], "partition": partition[1]}
        for partition in partitions_list
    ]
    return result


def query_with_retry(ctx, query, timeout, retries=10):
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


def reattach_partition(ctx, table, partition):

    logging.debug(f"Going to reattach partition {partition} for table {table}")
    detach_query = f"ALTER TABLE {table} DETACH PARTITION '{partition}'"
    attach_query = f"ALTER TABLE {table} ATTACH PARTITION '{partition}'"

    timeout = 5000
    retry_count = 10

    res = query_with_retry(ctx, detach_query, timeout=timeout, retries=retry_count)
    if not res:
        return res

    # To avoid keeping detached partitions, perform the attach query with double attempts.
    res = query_with_retry(ctx, attach_query, timeout=timeout, retries=2 * retry_count)
    if not res:
        return res
    return True
