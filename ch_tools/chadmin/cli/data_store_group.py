import os
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple

import xmltodict
from click import group, option, pass_context

from ch_tools.common import logging
from ch_tools.common.cli.formatting import print_response
from ch_tools.common.clickhouse.config import ClickhouseConfig

CLICKHOUSE_PATH = "/var/lib/clickhouse"
CLICKHOUSE_STORE_PATH = CLICKHOUSE_PATH + "/store"
CLICKHOUSE_DATA_PATH = CLICKHOUSE_PATH + "/data"
CLICKHOUSE_METADATA_PATH = CLICKHOUSE_PATH + "/metadata"


@group("data-store")
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
        path_result = process_path(path, prefix, column, remove)
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
    return subprocess.check_output(["du", "-sh", path]).split()[0].decode("utf-8")


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
        disk_conig = ClickhouseConfig.load().storage_configuration.get_disk_config(disk)
        disk_config_path = "/tmp/chadmin-ch-disks.xml"
        with open(disk_config_path, "w", encoding="utf-8") as f:
            xmltodict.unparse(
                {
                    "yandex": {
                        "storage_configuration": {"disks": {disk: disk_conig}},
                    }
                },
                f,
                pretty=True,
            )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Can't use map function here. The map method returns a generator
            # and it is not possible to resume a generator after an exception occurs.
            # https://peps.python.org/pep-0255/#specification-generators-and-exception-propagation
            if remove_only_metadata:
                futures_to_part = {
                    executor.submit(
                        remove_orphaned_sql_object_metadata,
                        data,
                    ): data
                    for data in lost_data
                }
            else:
                futures_to_part = {
                    executor.submit(
                        remove_orphaned_sql_object_full,
                        data,
                        disk,
                        path_to_disk,
                        disk_config_path,
                    ): data
                    for data in lost_data
                }
            for future in as_completed(futures_to_part):
                try:
                    future.result()
                except Exception as e:
                    if keep_going:
                        logging.warning(
                            "Ignoring the exception due to keep-going flag : {!r}", e
                        )
                    else:
                        raise

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


def remove_from_ch_disk(
    disk: str, path: str, disk_config_path: Optional[str] = None
) -> Tuple[int, bytes]:
    cmd = f"clickhouse-disks { '-C ' + disk_config_path if disk_config_path else ''} --disk {disk} remove {path}"
    logging.info("Run : {}", cmd)

    proc = subprocess.run(
        cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    return (proc.returncode, proc.stderr)


def remove_from_disk(path):
    cmd = f"rm -rf {path}"
    logging.info("Run : {}", cmd)

    proc = subprocess.run(
        cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    return (proc.returncode, proc.stderr)
