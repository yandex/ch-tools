"""
Check ClickHouse backups: its state, age and count.
"""

import json
from datetime import datetime, timedelta, timezone
from os.path import exists
from typing import Dict, List

import click
from dateutil.parser import parse as dateutil_parse

from ch_tools.common.backup import get_backups
from ch_tools.common.cli.parameters import TimeSpanParamType
from ch_tools.common.clickhouse.client import ClickhouseClient
from ch_tools.monrun_checks.exceptions import die

RESTORE_CONTEXT_PATH = "/tmp/ch_backup_restore_state.json"
FAILED_PARTS_THRESHOLD = 10


@click.command("backup")
@click.option(
    "--failed-backup-count-crit",
    "--failed-backup-count-crit-threshold",
    "--critical-failed",  # deprecated option name preserved for backward-compatibility
    "failed_backup_count_crit_threshold",
    default=3,
    help="Critical threshold on the number of failed backups. If last backups failed and its count equals or greater "
    "than the specified threshold, a crit will be reported.",
)
@click.option(
    "--backup-age-warn",
    "--backup-age-warn-threshold",
    "backup_age_warn_threshold",
    default="1d",
    type=TimeSpanParamType(),
    help="Warning threshold on age of the last backup. If the last backup is created more than the specified "
    "time ago, a warning will be reported.",
)
@click.option(
    "--backup-age-crit",
    "--backup-age-crit-threshold",
    "--critical-absent",  # deprecated option name preserved for backward-compatibility
    "backup_age_crit_threshold",
    default="2d",
    type=TimeSpanParamType(),
    help="Critical threshold on age of the last backup. If the last backup is created more than the specified "
    "time ago, a crit will be reported.",
)
@click.option(
    "--backup-count-warn",
    "--backup-count-warn-threshold",
    "backup_count_warn_threshold",
    default=0,
    help="Warning threshold on the number of backups. If the number of backups of any status equals or greater than "
    "the specified threshold, a warning will be reported.",
)
@click.option(
    "--min-uptime",
    "min_uptime",
    default="2d",
    type=TimeSpanParamType(),
    help="Minimal ClickHouse uptime enough for backup creation.",
)
def backup_command(
    failed_backup_count_crit_threshold,
    backup_age_warn_threshold,
    backup_age_crit_threshold,
    backup_count_warn_threshold,
    min_uptime,
):
    """
    Check ClickHouse backups: its state, age and count.
    """
    ch_client = ClickhouseClient()
    backups = get_backups()

    check_restored_data()
    check_uptime(ch_client, min_uptime)
    check_valid_backups_exist(backups)
    check_last_backup_not_failed(backups, failed_backup_count_crit_threshold)
    check_backup_age(backups, backup_age_warn_threshold, backup_age_crit_threshold)
    check_backup_count(backups, backup_count_warn_threshold)


def check_uptime(ch_client: ClickhouseClient, min_uptime: timedelta) -> None:
    """
    Check that ClickHouse uptime enough for backup creation.
    """
    uptime = ch_client.get_uptime()
    if uptime < min_uptime:
        die(0, "OK (forced due to low ClickHouse uptime)")


def check_valid_backups_exist(backups: List[Dict]) -> None:
    """
    Check that valid backups exist.
    """
    for backup in backups:
        if backup["state"] == "created":
            return

    die(2, "No valid backups found")


def check_last_backup_not_failed(backups: List[Dict], crit_threshold: int) -> None:
    """
    Check that the last backup is not failed. Its status must be 'created' or 'creating'.
    """
    counter = 0
    for i, backup in enumerate(backups):
        state = backup["state"]

        if state == "created":
            break

        if state == "failed" or (state == "creating" and i > 0):
            counter += 1

    if counter == 0:
        return

    if counter == 1:
        message = "Last backup failed"
    else:
        message = f"Last {counter} backups failed"

    status = 2 if crit_threshold and counter >= crit_threshold else 1
    die(status, message)


def check_backup_age(
    backups: List[Dict], warn_threshold: timedelta, crit_threshold: timedelta
) -> None:
    """
    Check that the last backup is not too old.
    """
    checking_backup: Dict[str, str] = {}
    for i, backup in enumerate(backups):
        state = backup["state"]
        if state == "created" or (state == "creating" and i == 0):
            checking_backup = backup
            break

    if len(checking_backup) == 0:
        die(2, "Didn't find a backup to check")

    backup_age = get_backup_age(checking_backup)

    if crit_threshold and backup_age >= crit_threshold:
        status = 2
    elif warn_threshold and backup_age >= warn_threshold:
        status = 1
    else:
        return

    if checking_backup["state"] == "creating":
        message = f"Last backup was started {backup_age.days} days ago"
    else:
        message = f"Last backup was created {backup_age.days} days ago"

    die(status, message)


def check_backup_count(backups: List[Dict], backup_count_warn_threshold: int) -> None:
    """
    Check that the number of backups is not too large.
    """
    backup_count = len(backups)
    if backup_count_warn_threshold and backup_count >= backup_count_warn_threshold:
        die(
            1, f"Too many backups exist: {backup_count} > {backup_count_warn_threshold}"
        )


def check_restored_data() -> None:
    """
    Check count of failed parts on restore
    """
    if exists(RESTORE_CONTEXT_PATH):
        with open(RESTORE_CONTEXT_PATH, "r", encoding="utf-8") as f:
            context = json.load(f)
            failed = sum(
                sum(len(table) for table in tables.values())
                for tables in context.get("failed", {}).get("failed_parts", {}).values()
            )
            restored = sum(
                sum(len(table) for table in tables.values())
                for tables in context["databases"].values()
            )
            if failed == 0:
                return
            failed_percent = int((failed / (failed + restored)) * 100)
            die(
                1 if failed_percent < FAILED_PARTS_THRESHOLD else 2,
                f"Some parts restore failed: {failed}({failed_percent}%)",
            )


def get_backup_age(backup):
    """
    Calculate and return age of ClickHouse backup.
    """
    backup_time = dateutil_parse(backup["start_time"])
    return datetime.now(timezone.utc) - backup_time
