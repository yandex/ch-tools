import os
import re
import shutil

from click import group, option


@group("disks")
def disks_group():
    """Commands to manage disks."""
    pass


@disks_group.command("check-s3-metadata")
@option(
    "--path",
    "path",
    default="/var/lib/clickhouse/disks/object_storage/store",
    help="Path to S3 metadata.",
)
@option("--cleanup", is_flag=True, help="Remove parts with corrupted S3 metadata.")
def check_s3_metadata_command(path, cleanup):
    check_dir(path, cleanup)


def check_dir(path, cleanup):
    corrupted_dirs = []
    for dirpath, _, filenames in os.walk(path):
        for filename in filenames:
            if not check_file(f"{dirpath}/{filename}"):
                print(f"{dirpath}/{filename}")
                if dirpath not in corrupted_dirs:
                    corrupted_dirs.append(dirpath)
    if cleanup:
        print("")
        for dirpath in corrupted_dirs:
            print(f'Remove directory "{dirpath}"')
            shutil.rmtree(dirpath)


def check_file(filename):
    with open(filename, mode="r", encoding="latin-1") as file:
        lines = file.readlines(1024)
        if len(lines) != 5:
            file.close()
            return False
        result = True
        if not re.match("[123]\n", lines[0]):  # version 1-3
            result = False
        elif not re.match("1\\s+\\d+\n", lines[1]):  # object count=1 & size
            result = False
        elif not re.match("\\d+\\s+\\S+\n", lines[2]):  # size & object name
            result = False
        elif not re.match("\\d+\n", lines[3]):  # refcount
            result = False
        elif not re.match("[01]\n?", lines[4]):  # is readonly
            result = False

    return result
