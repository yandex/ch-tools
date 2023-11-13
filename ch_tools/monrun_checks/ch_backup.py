"""
Check ClickHouse backups: its state, age and count.
"""

import json
import logging
import os.path
from datetime import datetime, timedelta, timezone
from os.path import exists
from typing import Dict, List

import click
from dateutil.parser import parse as dateutil_parse

from ch_tools.common.backup import get_backups
from ch_tools.common.cli.parameters import TimeSpanParamType
from ch_tools.common.clickhouse.client import ClickhouseClient
from ch_tools.common.result import CRIT, OK, WARNING, Result

LOAD_MONITOR_FLAG_PATH = "/tmp/load-monitor-userfault.flag"
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
    backups, result = _get_backups()

    if result.code == OK:
        result = _merge_results(
            _check_valid_backups_exist(backups),
            _check_last_backup_not_failed(backups, failed_backup_count_crit_threshold),
            _check_backup_age(
                backups, backup_age_warn_threshold, backup_age_crit_threshold
            ),
            _check_backup_count(backups, backup_count_warn_threshold),
            _check_restored_data(),
        )

    _suppress_if_required(result, min_uptime)

    return result


def _check_valid_backups_exist(backups: List[Dict]) -> Result:
    """
    Check that valid backups exist.
    """
    for backup in backups:
        if backup["state"] == "created":
            return Result(OK)

    return Result(CRIT, "No valid backups found")


def _check_last_backup_not_failed(backups: List[Dict], crit_threshold: int) -> Result:
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
        return Result(OK)

    if counter == 1:
        message = "Last backup failed"
    else:
        message = f"Last {counter} backups failed"

    status = CRIT if crit_threshold and counter >= crit_threshold else WARNING
    return Result(status, message)


def _check_backup_age(
    backups: List[Dict], warn_threshold: timedelta, crit_threshold: timedelta
) -> Result:
    """
    Check that the last backup is not too old.
    """
    checking_backup: Dict[str, str] = {}
    for i, backup in enumerate(backups):
        state = backup["state"]
        if state == "created" or (state == "creating" and i == 0):
            checking_backup = backup
            break

    if not checking_backup:
        return Result(OK)

    backup_age = _get_backup_age(checking_backup)

    if crit_threshold and backup_age >= crit_threshold:
        status = CRIT
    elif warn_threshold and backup_age >= warn_threshold:
        status = WARNING
    else:
        return Result(OK)

    if checking_backup["state"] == "creating":
        message = f"Last backup was started {backup_age.days} days ago"
    else:
        message = f"Last backup was created {backup_age.days} days ago"

    return Result(status, message)


def _check_backup_count(
    backups: List[Dict], backup_count_warn_threshold: int
) -> Result:
    """
    Check that the number of backups is not too large.
    """
    backup_count = len(backups)
    if backup_count_warn_threshold and backup_count >= backup_count_warn_threshold:
        return Result(
            WARNING,
            f"Too many backups exist: {backup_count} > {backup_count_warn_threshold}",
        )

    return Result(OK)


def _check_restored_data() -> Result:
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
            if failed != 0:
                failed_percent = int((failed / (failed + restored)) * 100)
                status = WARNING if failed_percent < FAILED_PARTS_THRESHOLD else CRIT
                return Result(
                    status, f"Some parts restore failed: {failed}({failed_percent}%)"
                )

    return Result(OK)


def _suppress_if_required(result: Result, min_uptime: timedelta) -> None:
    if result.code == OK:
        return

    if os.path.exists(LOAD_MONITOR_FLAG_PATH):
        result.code = WARNING
        result.message += " (suppressed by load monitor flag file)"

    if _get_uptime() < min_uptime:
        result.code = WARNING
        result.message += " (suppressed by low ClickHouse uptime)"


def _get_uptime() -> timedelta:
    try:
        return ClickhouseClient().get_uptime()
    except Exception:
        logging.warning("Failed to get ClickHouse uptime", exc_info=True)
        return timedelta()


def _get_backups():
    try:
        return get_backups(), Result(OK)
    except Exception:
        logging.exception("Failed to get backups")
        return None, Result(CRIT, "Failed to get backups")


def _get_backup_age(backup):
    """
    Calculate and return age of ClickHouse backup.
    """
    backup_time = dateutil_parse(backup["start_time"])
    return datetime.now(timezone.utc) - backup_time


def _merge_results(*results: Result) -> Result:
    merged_result = Result()
    for result in results:
        if result.code > merged_result.code:
            merged_result.code = result.code
            merged_result.message = result.message
            merged_result.verbose = result.verbose

    return merged_result
