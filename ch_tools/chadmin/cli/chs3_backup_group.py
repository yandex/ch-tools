import os

import requests
from click import ClickException, argument, group, option, pass_context

from ch_tools.chadmin.internal.backup import unfreeze_backup, unfreeze_table
from ch_tools.chadmin.internal.system import match_ch_version
from ch_tools.chadmin.internal.table import list_tables
from ch_tools.common.backup import (
    CHS3_BACKUPS_DIRECTORY,
    get_chs3_backups,
    get_orphaned_chs3_backups,
)
from ch_tools.common.utils import clear_empty_directories_recursively


@group("chs3-backup")
def chs3_backup_group():
    """Commands to manage ClickHouse over S3 backups (backups for data stored in S3)."""
    pass


@chs3_backup_group.command("list")
@option("--orphaned", is_flag=True)
def list_backups(orphaned):
    """List backups."""
    backups = get_orphaned_chs3_backups() if orphaned else get_chs3_backups()
    for backup in backups:
        print(backup)


@chs3_backup_group.command("delete")
@argument("backup")
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def delete_backup(ctx, backup, dry_run):
    """Delete backup."""
    chs3_backups = get_chs3_backups()
    if backup not in chs3_backups:
        raise ClickException(f"Backup {backup} not found.")

    delete_chs3_backups(ctx, [backup], dry_run=dry_run)


@chs3_backup_group.command("cleanup")
@option("-k", "--keep-going", is_flag=True, help="Do not stop on the first error.")
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def cleanup_backups(ctx, dry_run, keep_going):
    """Removed unnecessary / orphaned backups."""
    orphaned_chs3_backups = get_orphaned_chs3_backups()
    delete_chs3_backups(
        ctx, orphaned_chs3_backups, keep_going=keep_going, dry_run=dry_run
    )


def delete_chs3_backups(ctx, chs3_backups, *, keep_going=False, dry_run=False):
    """
    Delete CHS3 backups.
    """
    if match_ch_version(ctx, min_version="22.6"):
        _delete_chs3_backups(ctx, chs3_backups, keep_going=keep_going, dry_run=dry_run)
    else:
        _delete_chs3_backups_comp(ctx, chs3_backups, dry_run=dry_run)


def _delete_chs3_backups(ctx, chs3_backups, *, keep_going, dry_run):
    """
    Implementation of delete_chs3_backups() for ClickHouse versions greater or equal to 22.6 that
    support SYSTEM UNFREEZE query.
    """
    for chs3_backup in chs3_backups:
        try:
            unfreeze_backup(ctx, chs3_backup, dry_run=dry_run)
        except Exception as e:
            if keep_going:
                print(f"{e!r}\n")
            else:
                raise


def _delete_chs3_backups_comp(ctx, chs3_backups, *, dry_run):
    """
    Implementation of delete_chs3_backups() for ClickHouse versions lower 22.6 that
    do not support SYSTEM UNFREEZE query.
    """
    tables = get_tables_dict(ctx)
    for chs3_backup in chs3_backups:
        print(f"Removing backup: {chs3_backup}")
        for table in tables:
            try:
                unfreeze_table(
                    ctx,
                    database=table["database"],
                    table=table["table"],
                    backup_name=chs3_backup,
                    dry_run=dry_run,
                )
            except requests.exceptions.HTTPError:
                print(
                    f"Can't unfreeze table {table} in backup {chs3_backup}. Maybe it was deleted."
                )
            if not dry_run:
                clear_empty_backup(chs3_backup)


def get_tables_dict(ctx):
    tables = list_tables(ctx, engine="%MergeTree%")
    return [{"database": item["database"], "table": item["table"]} for item in tables]


def clear_empty_backup(orphaned_chs3_backup):
    backup_directory = os.path.join(CHS3_BACKUPS_DIRECTORY, orphaned_chs3_backup)
    try:
        backup_contents = os.listdir(backup_directory)
        clear_empty_directories_recursively(backup_directory)
        if len(os.listdir(backup_directory)) == 1 and "revision.txt" in backup_contents:
            os.remove(os.path.join(backup_directory, "revision.txt"))
            os.rmdir(backup_directory)
    except FileNotFoundError:
        print(
            f"Cannot remove backup directory {backup_directory} as it doesn`t exist. "
            f"Maybe it was already removed."
        )
