import os
import requests
from click import group, option, pass_context

from cloud.mdb.clickhouse.tools.common.utils import strip_query, clear_empty_directories_recursively
from cloud.mdb.clickhouse.tools.common.backup import get_orphaned_chs3_backups, get_chs3_backups, CHS3_BACKUPS_DIRECTORY

from cloud.mdb.clickhouse.tools.chadmin.cli import execute_query
from cloud.mdb.clickhouse.tools.chadmin.cli.tables import get_tables


@group('chs3-backup')
def chs3_backup_group():
    """Lost backups management commands."""
    pass


@chs3_backup_group.command('list')
@option('--orphaned', is_flag=True)
@pass_context
def list_backups(ctx, orphaned):
    backups = get_orphaned_chs3_backups() if orphaned else get_chs3_backups()
    for backup in backups:
        print(backup)


def get_tables_dict(ctx):
    tables_output = get_tables(ctx, database=None, format='JSON', engine='%MergeTree%')
    tables_output_dict = tables_output
    return [{
        'database': item['database'],
        'table': item['table']
    } for item in tables_output_dict['data']]


def clear_empty_backup(orphaned_chs3_backup):
    backup_directory = os.path.join(CHS3_BACKUPS_DIRECTORY, orphaned_chs3_backup)
    try:
        backup_contents = os.listdir(backup_directory)
        clear_empty_directories_recursively(backup_directory)
        if len(os.listdir(
                backup_directory)) == 1 and 'revision.txt' in backup_contents:
            os.remove(os.path.join(backup_directory, 'revision.txt'))
            os.rmdir(backup_directory)
    except FileNotFoundError:
        print(f"Cannot remove backup directory {backup_directory} as it doesn`t exist. "
              f"Maybe it was already removed.")


UNFREEZE_TABLE_SQL = strip_query("""
    ALTER TABLE `{db_name}`.`{table_name}`
    UNFREEZE WITH NAME '{backup_name}'
""")


def cleanup(ctx, orphaned_chs3_backups: [str], tables: [str], dry_run=True):
    for orphaned_chs3_backup in orphaned_chs3_backups:
        print(f'Removing backup: {orphaned_chs3_backup}')
        for table in tables:
            query = UNFREEZE_TABLE_SQL.format(
                db_name=table['database'],
                table_name=table['table'],
                backup_name=orphaned_chs3_backup)
            print(query)
            try:
                execute_query(ctx, query, dry_run=dry_run)
            except requests.exceptions.HTTPError:
                print(f"Can't unfreeze table {table} in backup {orphaned_chs3_backup}. Maybe it was deleted.")
        if not dry_run:
            clear_empty_backup(orphaned_chs3_backup)


@chs3_backup_group.command('cleanup')
@option('-n', '--dry-run', is_flag=True)
@pass_context
def cleanup_backups(ctx, dry_run):
    orphaned_chs3_backups = get_orphaned_chs3_backups()
    cleanup(ctx, orphaned_chs3_backups, get_tables_dict(ctx), dry_run)
