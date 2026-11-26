from click import ClickException, Context, IntRange, command, option, pass_context

from ch_tools.chadmin.internal.part import (
    drop_detached_part,
    list_detached_parts,
)
from ch_tools.common import logging


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
    eligible_parts = []
    skipped = 0
    for part in parts:
        if part["name"].startswith(("attaching_", "deleting_")):
            logging.warning("Skip service detached part: {}", part)
        elif part["disks_count"] > 1:
            logging.warning("Skip detached part present on multiple disks: {}", part)
        else:
            eligible_parts.append(part)
            continue
        skipped += 1

    processed = failed = 0
    for part in eligible_parts:
        try:
            # Use the server's DROP/ATTACH coordination for every part. A lost
            # response must not trigger another DROP of a replacement part.
            drop_detached_part(
                ctx,
                part["database"],
                part["table"],
                part["name"],
                dry_run=dry_run,
                retry_max_attempts=1,
            )
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
