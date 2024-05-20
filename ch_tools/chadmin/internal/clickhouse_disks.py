import subprocess
from typing import Optional, Tuple

import xmltodict

from ch_tools.common import logging
from ch_tools.common.clickhouse.config import ClickhouseConfig

CLICKHOUSE_PATH = "/var/lib/clickhouse"
CLICKHOUSE_STORE_PATH = CLICKHOUSE_PATH + "/store"
CLICKHOUSE_DATA_PATH = CLICKHOUSE_PATH + "/data"
CLICKHOUSE_METADATA_PATH = CLICKHOUSE_PATH + "/metadata"
S3_METADATA_STORE_PATH = CLICKHOUSE_PATH + "/disks/object_storage/store"


def make_ch_disks_config(disk: str) -> str:
    disk_config = ClickhouseConfig.load().storage_configuration.get_disk_config(disk)
    disk_config_path = f"/tmp/chadmin-ch-disks-{disk}.xml"
    logging.info("Create a conf for {} disk: {}", disk, disk_config_path)
    with open(disk_config_path, "w", encoding="utf-8") as f:
        xmltodict.unparse(
            {
                "yandex": {
                    "storage_configuration": {"disks": {disk: disk_config}},
                }
            },
            f,
            pretty=True,
        )
    return disk_config_path


def remove_from_ch_disk(
    disk: str, path: str, disk_config_path: Optional[str] = None
) -> Tuple[int, bytes]:
    cmd = f"clickhouse-disks { '-C ' + disk_config_path if disk_config_path else ''} --disk {disk} remove {path}"
    logging.info("Run : {}", cmd)

    proc = subprocess.run(
        cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    return (proc.returncode, proc.stderr)
