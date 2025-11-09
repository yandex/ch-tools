import click

from ch_tools.common.backup import get_orphaned_chs3_backups
from ch_tools.common.result import OK, WARNING, Result


@click.command("orphaned-backups")
def orphaned_backups_command() -> Result:
    """
    Check for orphaned backups.
    """
    orphaned_backups = get_orphaned_chs3_backups()
    if not orphaned_backups:
        return Result(OK)

    orphaned_backups_str = ", ".join(sorted(orphaned_backups)[:3])
    if len(orphaned_backups) > 3:
        orphaned_backups_str += ", ..."

    return Result(
        WARNING,
        f"There are {len(orphaned_backups)} orphaned S3 backups: {orphaned_backups_str}",
    )
