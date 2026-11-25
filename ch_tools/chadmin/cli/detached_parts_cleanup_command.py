from typing import Any

from click import ClickException, Context, IntRange, command, option, pass_context

from ch_tools.chadmin.internal.clickhouse_disks import remove_from_ch_disk
from ch_tools.chadmin.internal.part import (
    drop_detached_part,
    get_disks,
    list_detached_parts,
    part_has_suffix,
)
from ch_tools.chadmin.internal.system import get_version
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging


def _part_key(part: dict[str, Any]) -> tuple[str, str, str]:
    return part["database"], part["table"], part["name"]


def _ambiguous_part_names(ctx: Context) -> set[tuple[str, str, str]]:
    # Include fresh parts: DROP DETACHED PART chooses a disk by name, so an
    # expired copy must not cause a fresh copy on another disk to be removed.
    parts = execute_query(
        ctx,
        """
        SELECT database, table, name
        FROM system.detached_parts
        GROUP BY database, table, name
        HAVING uniqExact(disk) > 1
        """,
        format_="JSON",
    )["data"]
    return {_part_key(part) for part in parts}


def _delete_part(
    ctx: Context,
    part: dict[str, Any],
    disks: dict[str, dict[str, str]],
    dry_run: bool,
) -> None:
    if not part_has_suffix(part["name"]):
        drop_detached_part(
            ctx, part["database"], part["table"], part["name"], dry_run=dry_run
        )
        return

    disk = disks[part["disk_name"]]
    if not part["path"].startswith(disk["path"]):
        raise ValueError(f"Detached part path is outside its disk: {part['path']}")
    path_on_disk = part["path"][len(disk["path"]) :]
    returncode, stderr = remove_from_ch_disk(
        disk["name"], path_on_disk, get_version(ctx), dry_run=dry_run
    )
    if returncode:
        raise RuntimeError(
            f"clickhouse-disks failed with code {returncode}: "
            f"{stderr.decode(errors='replace')}"
        )


@command("detached-parts-cleanup")
@option(
    "--automatically-detached-data-parts-retention-time",
    type=IntRange(min=0),
    help="Retention time for automatically detached data parts in seconds. "
    "Zero disables cleanup of these parts.",
)
@option(
    "--manually-detached-data-parts-retention-time",
    type=IntRange(min=0),
    help="Retention time for manually detached data parts in seconds. "
    "Zero disables cleanup of these parts.",
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def detached_parts_cleanup_command(
    ctx: Context,
    automatically_detached_data_parts_retention_time: int,
    manually_detached_data_parts_retention_time: int,
    dry_run: bool,
) -> None:
    """Clean up expired detached data parts."""
    if (
        automatically_detached_data_parts_retention_time == 0
        and manually_detached_data_parts_retention_time == 0
    ):
        logging.info("Detached parts cleanup is disabled")
        return

    parts = list_detached_parts(
        ctx,
        empty_reason_min_age=manually_detached_data_parts_retention_time,
        non_empty_reason_min_age=automatically_detached_data_parts_retention_time,
    )
    ambiguous_names = _ambiguous_part_names(ctx) if parts else set()
    eligible_parts = []
    skipped = 0
    for part in parts:
        if part["name"].startswith(("attaching_", "deleting_")):
            logging.warning("Skip service detached part: {}", part)
        elif _part_key(part) in ambiguous_names:
            logging.warning("Skip detached part present on multiple disks: {}", part)
        else:
            eligible_parts.append(part)
            continue
        skipped += 1

    disks = get_disks(ctx) if eligible_parts else {}
    processed = failed = 0
    for part in eligible_parts:
        try:
            _delete_part(ctx, part, disks, dry_run=dry_run)
            processed += 1
        except Exception as error:
            logging.error("Failed to delete detached part {}: {!r}", part, error)
            failed += 1

    logging.info(
        "Detached parts cleanup{}: processed={}, skipped={}, failed={}",
        " (dry run)" if dry_run else "",
        processed,
        skipped,
        failed,
    )
    if failed:
        raise ClickException(f"Failed to delete {failed} detached data part(s)")
