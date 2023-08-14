import os
import shutil
import subprocess
from typing import Optional

from click import group, option

CLICKHOUSE_PATH = "/var/lib/clickhouse"
CLICKHOUSE_STORE_PATH = CLICKHOUSE_PATH + "/store"


@group("data-store")
def data_store_group():
    """
    Commands for manipulating data stored by ClickHouse.
    """
    pass


@data_store_group.command("clean-orphaned-tables")
@option(
    "--column",
    "column",
    default="",
    help="Additional check: specified COLUMN name should exists in data to be removed. Example: `initial_query_start_time_microseconds.bin` for `query_log`-table.",
)
@option(
    "--remove",
    is_flag=True,
    default=False,
    help="Flag to REMOVE data from store subdirectories.",
)
def clean_orphaned_tables_command(column, remove):
    for prefix in os.listdir(CLICKHOUSE_STORE_PATH):
        path = CLICKHOUSE_STORE_PATH + "/" + prefix

        process_path(path, prefix, column, remove)


def process_path(path: str, prefix: str, column: str, remove: bool) -> None:
    print(f"Processing path {path} with prefix {prefix}:")

    file = prefix_exists_in_metadata(prefix)
    if file:
        print(f'\tPrefix "{prefix}" is used in metadata file "{file}"')
        return

    print(f'\tPrefix "{prefix}" is NOT used in any metadata file')

    if not additional_check_successed(column, path):
        print("\t\tAdditional check for column-parameter not passed")
        return

    size = du(path)

    print(f"\t\tSize of path {path}: {size}")

    if remove:
        print(f'\t\tTrying to remove path "{path}"...')

        remove_data(path)
        return

    print(
        f'\t\tPath "{path}" is not removed because of remove-parameter is not specified'
    )


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
    if not column:
        return False

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
        print(f"\t\t\tERROR! {errors}")

    shutil.rmtree(path=path, onerror=onerror)
