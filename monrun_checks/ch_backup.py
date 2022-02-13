"""
Check ClickHouse backups: its state, age and count.
"""

import json
from datetime import datetime, timezone
from os.path import exists

import click

from cloud.mdb.clickhouse.tools.common.backup import BackupConfig, get_backups
from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseClient
from cloud.mdb.clickhouse.tools.monrun_checks.exceptions import die

DATE_FORMAT = '%Y-%m-%d %H:%M:%S %z'
RESTORE_CONTEXT_PATH = '/tmp/ch_backup_restore_state.json'
FAILED_PARTS_THRESHOLD = 10


@click.command('backup')
def backup_command():
    """
    Check ClickHouse backups: its state, age and count.
    """
    ch_client = ClickhouseClient()
    backup_config = BackupConfig.load()
    backups = get_backups()

    check_restored_parts()
    check_valid_backups_exist(backups)
    check_last_backup_not_failed(backups)
    check_backup_age(ch_client, backups)
    check_backup_count(backup_config, backups)


def check_valid_backups_exist(backups):
    """
    Check that valid backups exist.
    """
    for backup in backups:
        if backup['state'] == 'created':
            return

    die(2, 'No valid backups found')


def check_last_backup_not_failed(backups):
    """
    Check that the last backup is not failed. Its status must be 'created' or 'creating'.
    """
    counter = 0
    for i, backup in enumerate(backups):
        state = backup['state']

        if state == 'created':
            break

        if state == 'failed' or (state == 'creating' and i > 0):
            counter += 1

    if counter == 0:
        return

    status = 2 if counter >= 3 else 1
    if counter > 1:
        message = f'Last {counter} backups failed'
    else:
        message = 'Last backup failed'
    die(status, message)


def check_backup_age(ch_client, backups, age_threshold=1):
    """
    Check that the last backup is not too old.
    """
    # To avoid false warnings the check is skipped if ClickHouse uptime is less then age threshold.
    if ch_client.get_uptime().days < age_threshold:
        return

    checking_backup = None
    for i, backup in enumerate(backups):
        state = backup['state']
        if state == 'created' or (state == 'creating' and i == 0):
            checking_backup = backup
            break

    backup_age = get_backup_age(checking_backup)
    if backup_age.days < age_threshold:
        return

    if checking_backup['state'] == 'creating':
        message = f'Last backup was started {backup_age.days} days ago'
    else:
        message = f'Last backup was created {backup_age.days} days ago'
    die(1, message)


def check_backup_count(config: BackupConfig, backups: list) -> None:
    """
    Check that the number of backups is not too large.
    """
    max_count = config.retain_count + config.deduplication_age_limit.days + 1

    count = len(backups)
    if count > max_count:
        die(1, f'Too many backups exist: {count} > {max_count}')


def check_restored_parts() -> None:
    """
    Check count of failed parts on restore
    """
    if exists(RESTORE_CONTEXT_PATH):
        with open(RESTORE_CONTEXT_PATH, 'r') as f:
            context = json.load(f)
            failed = sum(
                sum(len(table) for table in tables.values())
                for tables in context.get('failed', {}).get('failed_parts', {}).values()
            )
            restored = sum(sum(len(table) for table in tables.values()) for tables in context['databases'].values())
            if failed == 0:
                return
            failed_percent = int((failed / (failed + restored)) * 100)
            die(
                1 if failed_percent < FAILED_PARTS_THRESHOLD else 2,
                f'Some parts restore failed: {failed}({failed_percent}%)',
            )


def get_backup_age(backup):
    """
    Calculate and return age of ClickHouse backup.
    """
    backup_time = datetime.strptime(backup['start_time'], DATE_FORMAT)
    return datetime.now(timezone.utc) - backup_time
