from datetime import timedelta
from typing import Optional

import click

from ch_tools.common.cli.parameters import TimeSpanParamType
from ch_tools.common.clickhouse.config import get_clickhouse_config
from ch_tools.common.commands.clean_object_storage import DEFAULT_GUARD_INTERVAL, clean
from ch_tools.common.result import CRIT, OK, WARNING, Result

DRY_RUN = True
ON_CLUSTER = True
CLUSTER_NAME = "{cluster}"
DISK_NAME = "object_storage"
OBJECT_NAME_PREFIX = ""


@click.command("orphaned-objects")
@click.option(
    "--keep-paths",
    "keep_paths",
    is_flag=True,
    help=(
        "Do not delete the collected paths from the local table, when the command completes."
    ),
)
@click.option(
    "--use-saved-list",
    "use_saved_list",
    is_flag=True,
    help=("Use saved object list without traversing object storage again."),
)
@click.option(
    "-c",
    "--critical",
    "crit",
    type=int,
    default=10 * 1024**3,
    help="Critical threshold.",
)
@click.option(
    "-w",
    "--warning",
    "warn",
    type=int,
    default=100 * 1024**2,
    help="Warning threshold.",
)
@click.option(
    "--from-time",
    "from_time",
    default=None,
    type=TimeSpanParamType(),
    help=(
        "Begin of inspecting interval in human-friendly format. "
        "Objects with a modification time falling interval [now - from_time, now - to_time] are considered."
    ),
)
@click.option(
    "-g",
    "--guard-interval",
    "--to-time",
    "to_time",
    default=DEFAULT_GUARD_INTERVAL,
    type=TimeSpanParamType(),
    help=("End of inspecting interval in human-friendly format."),
)
@click.pass_context
def orphaned_objects_command(
    ctx: click.Context,
    keep_paths: bool,
    use_saved_list: bool,
    crit: int,
    warn: int,
    from_time: Optional[timedelta],
    to_time: timedelta,
) -> Result:
    ch_config = get_clickhouse_config(ctx)
    ctx.obj[
        "disk_configuration"
    ] = ch_config.storage_configuration.s3_disk_configuaration(DISK_NAME)

    _, total_size = clean(
        ctx,
        OBJECT_NAME_PREFIX,
        from_time,
        to_time,
        ON_CLUSTER,
        CLUSTER_NAME,
        DRY_RUN,
        keep_paths,
        use_saved_list,
    )

    msg = f"Total size: {total_size}"
    if total_size >= crit:
        return Result(CRIT, msg)
    if total_size >= warn:
        return Result(WARNING, msg)
    return Result(OK, msg)
