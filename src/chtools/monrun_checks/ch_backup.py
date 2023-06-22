"""
Check ClickHouse backups: its state, age and count.
"""

import json
from datetime import datetime, timezone
from os.path import exists
from typing import Dict, List

import click

from chtools.common.backup import BackupConfig, get_backups
from chtools.common.clickhouse.client import ClickhouseClient
from chtools.common.dbaas import DbaasConfig
from chtools.monrun_checks.exceptions import die

DATE_FORMAT = '%Y-%m-%d %H:%M:%S %z'
FULL_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%f%z'
RESTORE_CONTEXT_PATH = '/tmp/ch_backup_restore_state.json'
FAILED_PARTS_THRESHOLD = 10


@click.command('backup')
@click.option('--critical-failed', 'crit_failed', default=3, help='Critical threshold for failed backupd.')
@click.option('--critical-absent', 'crit_absent', default=0, help='Critical threshold for absent backupd')
def backup_command(crit_failed, crit_absent):
    """
    Check ClickHouse backups: its state, age and count.
    Skip backup checks for recently created clusters.
    """
    dbaas_config = DbaasConfig.load()

    cluster_created_at = parse_str_datetime(dbaas_config.created_at)
    if not check_now_pass_threshold(cluster_created_at):
        return

    ch_client = ClickhouseClient()
    backup_config = BackupConfig.load()
    backups = get_backups()

    check_restored_parts()
    check_valid_backups_exist(backups)
    check_last_backup_not_failed(backups, crit=crit_failed)
    check_backup_age(ch_client, backups, crit=crit_absent)
    check_backup_count(backup_config, backups)


def check_valid_backups_exist(backups):
    """
    Check that valid backups exist.
    """
    for backup in backups:
        if backup['state'] == 'created':
            return

    die(2, 'No valid backups found')


def check_last_backup_not_failed(backups, crit=3):
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

    status = 2 if crit > 0 and counter >= crit else 1
    if counter > 1:
        message = f'Last {counter} backups failed'
    else:
        message = 'Last backup failed'
    die(status, message)


def check_backup_age(ch_client, backups, age_threshold=1, crit=0):
    """
    Check that the last backup is not too old.
    """
    # To avoid false warnings the check is skipped if ClickHouse uptime is less then age threshold.
    uptime = ch_client.get_uptime().days
    if uptime < age_threshold:
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
    status = 1
    if crit > 0 and uptime >= crit and backup_age.days >= crit:
        status = 2
    die(status, message)


def check_backup_count(config: BackupConfig, backups: List[Dict]) -> None:
    """
    Check that the number of backups is not too large.
    """
    max_count = config.retain_count + config.deduplication_age_limit.days + 2  # backup-service backup retention delay

    count = sum(1 for backup in backups if backup.get('labels', {}).get('user_initiator'))
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


def parse_str_datetime(input: str) -> datetime:
    """
    Parse input string to datetime.
    """
    if input is None:
        return None

    try:
        return datetime.strptime(input, FULL_DATE_FORMAT)
    except Exception:
        return None


def check_now_pass_threshold(date_time: datetime, hours_threshold: int = 25) -> bool:
    """
    Check that hours threshold is passed since input date
    """
    if date_time is None:
        return True

    diff = datetime.now(date_time.tzinfo) - date_time

    days, seconds = diff.days, diff.seconds
    diff_in_hours = days * 24 + seconds // (60 * 60)

    return diff_in_hours >= hours_threshold
