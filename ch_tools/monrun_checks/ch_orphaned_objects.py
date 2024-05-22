from tempfile import TemporaryFile
import click

from ch_tools.chadmin.internal.object_storage.obj_list_item import ObjListItem
from ch_tools.chadmin.internal.object_storage.s3_cleanup import cleanup_s3_object_storage
from ch_tools.chadmin.internal.object_storage.utils import DEFAULT_GUARD_INTERVAL, get_orphaned_objects_query, get_remote_data_paths_table, get_traverse_shadow_settings, traverse_object_storage
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common.cli.utils import parse_timespan
from ch_tools.common.clickhouse.config import get_clickhouse_config
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration
from ch_tools.common.result import CRIT, OK, WARNING, Result

FROM_TIME = None
TO_TIME = parse_timespan(DEFAULT_GUARD_INTERVAL)
DRY_RUN = True
ON_CLUSTER = True
CLUSTER_NAME = "{cluster}"
DIST_NAME = "object_storage"
OBJECT_NAME_PREFIX = None


@click.command("orphaned-objects")
@click.option(
    "--keep-paths",
    "keep_paths",
    is_flag=True,
    help=("Do not delete collected paths of objects from object storage."),
)
@click.option(
    "--use-saved-list",
    "use_saved_list",
    is_flag=True,
    help=("Use saved object list without traversing object storage again."),
)
@click.option(
    "-c", "--critical", "crit", type=int, default=3600, help="Critical threshold."
)
@click.option(
    "-w", "--warning", "warn", type=int, default=600, help="Warning threshold."
)
@click.pass_context
def orphaned_objects_command(ctx: click.Context, keep_paths: bool, use_saved_list: bool, crit: int, warn: int):
    ch_config = get_clickhouse_config(ctx)
    ctx.obj[
        "disk_configuration"
    ] = ch_config.storage_configuration.s3_disk_configuaration(DIST_NAME)

    if FROM_TIME is not None and FROM_TIME <= TO_TIME:
        raise click.BadParameter(
            "'to_time' parameter must be greater than 'from_time'",
            param_hint="--from-time",
        )

    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]
    config = ctx.obj["config"]["object_storage"]["clean"]

    listing_table = f"{config['listing_table_database']}.{config['listing_table_prefix']}{disk_conf.name}"
    #Create listing table for storing paths from object storage
    try:
        execute_query(
            ctx,
            f"CREATE TABLE IF NOT EXISTS {listing_table} (obj_path String, obj_size UInt64) ENGINE MergeTree ORDER BY obj_path SETTINGS storage_policy = '{config['storage_policy']}'",
        )
        total_size = _get_total_size(
            ctx,
            listing_table,
            use_saved_list,
        )
    finally:
        if not keep_paths:
            execute_query(
                ctx, f"DROP TABLE IF EXISTS {listing_table} SYNC", format_=None
            )

    msg = f"Total size: {total_size}"
    if total_size >= crit:
        return Result(CRIT, msg)
    if total_size >= warn:
        return Result(WARNING, msg)
    return Result(OK, msg)


def _get_total_size(
    ctx: click.Context,
    listing_table: str,
    use_saved_list: bool,
) -> None:
    """
    Delete orphaned objects from object storage.
    """
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]
    prefix = OBJECT_NAME_PREFIX or disk_conf.prefix

    if not use_saved_list:
        _ = traverse_object_storage(ctx, listing_table, FROM_TIME, TO_TIME, prefix)

    remote_data_paths_table = get_remote_data_paths_table(ON_CLUSTER, CLUSTER_NAME)
    settings = get_traverse_shadow_settings(ctx)
    antijoin_query = get_orphaned_objects_query(listing_table, remote_data_paths_table, disk_conf, settings)

    total_size = 0
    with TemporaryFile() as keys_file:
        with execute_query(
            ctx, antijoin_query, stream=True, format_="TabSeparated"
        ) as resp:
            # Save response to the file by chunks
            for chunk in resp.iter_content(chunk_size=8192):
                keys_file.write(chunk)

        keys_file.seek(0)  # rewind file pointer to the beginning

        keys = (
            ObjListItem.from_tab_separated(line.decode().strip()) for line in keys_file
        )
        _, total_size = cleanup_s3_object_storage(disk_conf, keys, DRY_RUN)

    return total_size
