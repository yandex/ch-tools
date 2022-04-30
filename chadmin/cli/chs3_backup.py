import os

import requests
from click import argument, ClickException, group, option, pass_context

from cloud.mdb.clickhouse.tools.chadmin.cli import execute_query
from cloud.mdb.clickhouse.tools.chadmin.cli.tables import get_tables
from cloud.mdb.clickhouse.tools.common.backup import CHS3_BACKUPS_DIRECTORY, get_chs3_backups, get_orphaned_chs3_backups
from cloud.mdb.clickhouse.tools.common.utils import clear_empty_directories_recursively, strip_query

UNFREEZE_TABLE_SQL = strip_query(
    """
    ALTER TABLE `{db_name}`.`{table_name}`
    UNFREEZE WITH NAME '{backup_name}'
"""
)


@group('chs3-backup')
def chs3_backup_group():
    """Commands to manage ClickHouse over S3 backups (backups for data stored in S3)."""
    pass


@chs3_backup_group.command('list')
@option('--orphaned', is_flag=True)
def list_backups(orphaned):
    """List backups."""
    backups = get_orphaned_chs3_backups() if orphaned else get_chs3_backups()
    for backup in backups:
        print(backup)


@chs3_backup_group.command('delete')
@argument('backup')
@option('-n', '--dry-run', is_flag=True)
@pass_context
def delete_backup(ctx, backup, dry_run):
    """Delete backup."""
    chs3_backups = get_chs3_backups()
    if backup not in chs3_backups:
        raise ClickException(f'Backup {backup} not found.')

    delete_chs3_backups(ctx, [backup], dry_run)


@chs3_backup_group.command('cleanup')
@option('-n', '--dry-run', is_flag=True)
@pass_context
def cleanup_backups(ctx, dry_run):
    """Removed unnecessary / orphaned backups."""
    orphaned_chs3_backups = get_orphaned_chs3_backups()
    delete_chs3_backups(ctx, orphaned_chs3_backups, dry_run)


def delete_chs3_backups(ctx, chs3_backups: [str], dry_run=True):
    tables = get_tables_dict(ctx)
    for chs3_backup in chs3_backups:
        print(f'Removing backup: {chs3_backup}')
        for table in tables:
            query = UNFREEZE_TABLE_SQL.format(
                db_name=table['database'], table_name=table['table'], backup_name=chs3_backup
            )
            print(query)
            try:
                execute_query(ctx, query, dry_run=dry_run)
            except requests.exceptions.HTTPError:
                print(f"Can't unfreeze table {table} in backup {chs3_backup}. Maybe it was deleted.")
        if not dry_run:
            clear_empty_backup(chs3_backup)


def get_tables_dict(ctx):
    tables_output = get_tables(ctx, database=None, format='JSON', engine='%MergeTree%')
    tables_output_dict = tables_output
    return [{'database': item['database'], 'table': item['table']} for item in tables_output_dict['data']]


def clear_empty_backup(orphaned_chs3_backup):
    backup_directory = os.path.join(CHS3_BACKUPS_DIRECTORY, orphaned_chs3_backup)
    try:
        backup_contents = os.listdir(backup_directory)
        clear_empty_directories_recursively(backup_directory)
        if len(os.listdir(backup_directory)) == 1 and 'revision.txt' in backup_contents:
            os.remove(os.path.join(backup_directory, 'revision.txt'))
            os.rmdir(backup_directory)
    except FileNotFoundError:
        print(
            f"Cannot remove backup directory {backup_directory} as it doesn`t exist. " f"Maybe it was already removed."
        )
