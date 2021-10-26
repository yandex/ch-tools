import subprocess
import os

backups_directory = '/var/lib/clickhouse/disks/object_storage/shadow/'


def get_backups() -> [str]:
    result = []
    ch_backup_process = ['ch-backup', 'list', '-a']
    output = subprocess.run(ch_backup_process, stdout=subprocess.PIPE, universal_newlines=True)
    for index, line in enumerate(output.stdout.split('\n')):
        line = line.strip()
        if line:
            result.append(line)
    return result


def get_chs3_backups() -> [str]:
    if os.path.exists(backups_directory):
        return os.listdir(backups_directory)
    else:
        return []


def get_orphaned_chs3_backups() -> [str]:
    backups = get_backups()
    chs3_backups = get_chs3_backups()
    return list(set(chs3_backups) - set(backups))
