import subprocess
from typing import Optional, Tuple

import xmltodict

from ch_tools.common import logging
from ch_tools.common.clickhouse.config import ClickhouseConfig
from ch_tools.common.utils import version_ge

CLICKHOUSE_PATH = "/var/lib/clickhouse"
CLICKHOUSE_STORE_PATH = CLICKHOUSE_PATH + "/store"
CLICKHOUSE_DATA_PATH = CLICKHOUSE_PATH + "/data"
CLICKHOUSE_METADATA_PATH = CLICKHOUSE_PATH + "/metadata"
S3_PATH = CLICKHOUSE_PATH + "/disks/object_storage"
S3_METADATA_STORE_PATH = S3_PATH + "/store"

OBJECT_STORAGE_DISK_TYPES = ["s3", "object_storage", "ObjectStorage"]


def make_ch_disks_config(disk: str) -> str:
    disk_config = ClickhouseConfig.load().storage_configuration.get_disk_config(disk)
    disk_config_path = f"/tmp/chadmin-ch-disks-{disk}.xml"
    logging.info("Create a conf for {} disk: {}", disk, disk_config_path)
    with open(disk_config_path, "w", encoding="utf-8") as f:
        xmltodict.unparse(
            {
                "clickhouse": {
                    "storage_configuration": {"disks": {disk: disk_config}},
                }
            },
            f,
            pretty=True,
        )
    return disk_config_path


def remove_from_ch_disk(
    disk: str,
    path: str,
    ch_version: str,
    disk_config_path: Optional[str] = None,
    dry_run: bool = False,
) -> Tuple[int, bytes]:
    cmd = f"clickhouse-disks {'-C ' + disk_config_path if disk_config_path else ''} --disk {disk}"
    if version_ge(ch_version, "24.7"):
        cmd += f' --query "remove {path} --recursive"'
    else:
        cmd += f" remove {path}"

    logging.info("Run : {}", cmd)

    if dry_run:
        return (0, b"")

    proc = subprocess.run(
        cmd,
        shell=True,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    logging.info(
        "clickhouse-disks remove command has finished: retcode {}, stderr: {}",
        proc.returncode,
        proc.stderr.decode(),
    )
    return (proc.returncode, proc.stderr)
