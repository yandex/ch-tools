import click

from ch_tools.common.backup import get_orphaned_chs3_backups
from ch_tools.common.result import Result


@click.command("orphaned-backups")
def orphaned_backups_command():
    """
    Check for orphaned backups.
    """
    orphaned_backups = get_orphaned_chs3_backups()
    if not orphaned_backups:
        return Result(0, "OK")

    orphaned_backups_str = ", ".join(orphaned_backups[:3])
    if len(orphaned_backups) > 3:
        orphaned_backups_str += ", ..."

    return Result(
        1,
        f"There are {len(orphaned_backups)} orphaned S3 backups: {orphaned_backups_str}",
    )
