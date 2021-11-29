import click

from cloud.mdb.clickhouse.tools.common.backup import get_orphaned_chs3_backups

from cloud.mdb.clickhouse.tools.monrun_checks.result import Result


@click.command('orphaned-backups')
def orphaned_backups_command():
    """
    Check for orphaned backups.
    """
    orphaned_backups = get_orphaned_chs3_backups()
    if len(orphaned_backups) > 0:
        return Result(1, f'There are {len(orphaned_backups)} orphaned S3 backups: {orphaned_backups}')
    else:
        return Result(0, 'OK')
