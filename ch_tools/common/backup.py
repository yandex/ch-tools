import json
import os
import subprocess
from datetime import timedelta
from typing import Any, Dict, List, Optional

import yaml

from ch_tools.common.clickhouse.client.retry import retry

CHS3_BACKUPS_DIRECTORY = "/var/lib/clickhouse/disks/object_storage/shadow/"


class BackupConfig:
    """
    Configuration of ch-backup tool.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self._config = config

    @property
    def deduplication_age_limit(self) -> timedelta:
        return timedelta(**self._config["backup"]["deduplication_age_limit"])

    @property
    def retain_count(self) -> int:
        return self._config["backup"]["retain_count"]

    @staticmethod
    def load() -> "BackupConfig":
        with open(
            "/etc/yandex/ch-backup/ch-backup.conf", "r", encoding="utf-8"
        ) as file:
            return BackupConfig(yaml.safe_load(file))


@retry(json.decoder.JSONDecodeError)
def get_backups() -> List[Dict[str, Any]]:
    """
    Get ClickHouse backups.
    """
    return json.loads(run("sudo ch-backup list -a -v --format json"))


def get_chs3_backups() -> List[str]:
    if os.path.exists(CHS3_BACKUPS_DIRECTORY):
        return os.listdir(CHS3_BACKUPS_DIRECTORY)

    return []


def get_orphaned_chs3_backups() -> List[str]:
    backups = get_backups()
    chs3_backups = get_chs3_backups()
    return list(set(chs3_backups) - set(backup["name"] for backup in backups))


def run(command: str, data: Optional[str] = None) -> str:
    """
    Run the command and return its output.
    """
    # pylint: disable=consider-using-with

    proc = subprocess.Popen(
        command,
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    encoded_data = data.encode() if data else None

    stdout, stderr = proc.communicate(input=encoded_data)

    if proc.returncode:
        message = f'Command "{command}" failed with code {proc.returncode}'
        if stderr:
            message = f"{message}\n{stderr.decode().strip()}"
        raise RuntimeError(message)

    return stdout.decode()
