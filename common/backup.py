import subprocess
import os
from datetime import timedelta

import yaml

CHS3_BACKUPS_DIRECTORY = '/var/lib/clickhouse/disks/object_storage/shadow/'


class BackupConfig:
    """
    Configuration of ch-backup tool.
    """

    def __init__(self, config):
        self._config = config

    @property
    def deduplication_age_limit(self):
        return timedelta(**self._config['backup']['deduplication_age_limit'])

    @property
    def retain_count(self):
        return self._config['backup']['retain_count']

    @staticmethod
    def load():
        with open('/etc/yandex/ch-backup/ch-backup.conf', 'r') as file:
            return BackupConfig(yaml.safe_load(file))


def get_backups() -> [str]:
    result = []
    ch_backup_process = ['sudo', 'ch-backup', 'list', '-a']
    output = subprocess.run(ch_backup_process, stdout=subprocess.PIPE, universal_newlines=True)
    for index, line in enumerate(output.stdout.split('\n')):
        line = line.strip()
        if line:
            result.append(line)
    return result


def get_chs3_backups() -> [str]:
    if os.path.exists(CHS3_BACKUPS_DIRECTORY):
        return os.listdir(CHS3_BACKUPS_DIRECTORY)
    else:
        return []


def get_orphaned_chs3_backups() -> [str]:
    backups = get_backups()
    chs3_backups = get_chs3_backups()
    return list(set(chs3_backups) - set(backups))
