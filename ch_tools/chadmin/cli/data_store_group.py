import logging
import os
import shutil
import subprocess
from typing import Optional

from click import group, option, pass_context

from ch_tools.common.cli.formatting import print_response

CLICKHOUSE_PATH = "/var/lib/clickhouse"
CLICKHOUSE_STORE_PATH = CLICKHOUSE_PATH + "/store"


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
    logging.info("Processing path %s with prefix %s:", path, prefix)

    result = {
        "path": path,
        "status": "unknown",
        "size": 0,
        "removed": False,
    }

    size = du(path)
    logging.info("Size of path %s: %s", path, size)
    result["size"] = size

    file = prefix_exists_in_metadata(prefix)

    if file:
        logging.info('Prefix "%s" is used in metadata file "%s"', prefix, file)
        result["status"] = "used"
        return result

    if column and not additional_check_successed(column, path):
        logging.info("Additional check for column-parameter not passed")
        result["status"] = "not_passed_column_check"
        return result

    logging.info('Prefix "%s" is NOT used in any metadata file', prefix)
    result["status"] = "not_used"

    if remove:
        logging.info('Trying to remove path "%s"', path)

        remove_data(path)
        result["removed"] = True
    else:
        logging.info(
            'Path "%s" is not removed because of remove-parameter is not specified',
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
        errors = list(args)
        logging.error("ERROR: %s", "\n".join(errors))

    shutil.rmtree(path=path, onerror=onerror)
