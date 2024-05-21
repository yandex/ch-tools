from datetime import datetime, timedelta, timezone
from typing import List, Optional
from click import Context

from ch_tools.chadmin.internal.object_storage.obj_list_item import ObjListItem
from ch_tools.chadmin.internal.object_storage.s3_iterator import s3_object_storage_iterator
from ch_tools.chadmin.internal.system import match_ch_version
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration

# Batch size for inserts in a listing table
# Set not very big value due to default ClickHouse 'http_max_field_value_size' settings value 128Kb
# TODO: streaming upload in POST body while INSERT
INSERT_BATCH_SIZE = 500


def get_remote_data_paths_table(on_cluster: bool, cluster_name: str) -> str:
    remote_data_paths_table = "system.remote_data_paths"
    if on_cluster:
        remote_data_paths_table = (
            f"clusterAllReplicas('{cluster_name}', {remote_data_paths_table})"
        )
    return remote_data_paths_table


def get_traverse_shadow_settings(ctx: Context) -> str:
    settings = ""
    if match_ch_version(ctx, min_version="24.3"):
        settings = "SETTINGS traverse_shadow_remote_data_paths=1"
    return settings


def get_orphaned_objects_query(listing_table: str, remote_data_paths_table: str, disk_conf: S3DiskConfiguration, settings: str) -> str:
    query = f"""
        SELECT obj_path, obj_size FROM {listing_table} AS object_storage
          LEFT ANTI JOIN {remote_data_paths_table} AS object_table
          ON object_table.remote_path = object_storage.obj_path
            AND object_table.disk_name = '{disk_conf.name}'
        {settings}
    """
    return query


def traverse_object_storage(
    ctx: Context,
    listing_table: str,
    from_time: Optional[timedelta],
    to_time: timedelta,
    prefix: str,
) -> int:
    """
    Traverse S3 disk's bucket and put object names to the ClickHouse table.
    """
    obj_paths_batch: List[ObjListItem] = []
    counter = 0
    now = datetime.now(timezone.utc)

    for obj in s3_object_storage_iterator(
        ctx.obj["disk_configuration"], object_name_prefix=prefix
    ):
        if obj.last_modified > now - to_time:
            continue
        if from_time is not None and obj.last_modified < now - from_time:
            continue

        obj_paths_batch.append(ObjListItem(obj.key, obj.size))
        counter += 1
        if len(obj_paths_batch) >= INSERT_BATCH_SIZE:
            _insert_listing_batch(ctx, obj_paths_batch, listing_table)
            obj_paths_batch.clear()

    # Insert the last batch (might be shorter)
    if obj_paths_batch:
        _insert_listing_batch(ctx, obj_paths_batch, listing_table)
    
    return counter


def _insert_listing_batch(
    ctx: Context, obj_paths_batch: List[ObjListItem], listing_table: str
) -> None:
    """
    Insert batch of object names to the listing table.
    """
    batch_values = ",".join(f"('{item.path}',{item.size})" for item in obj_paths_batch)
    execute_query(
        ctx,
        f"INSERT INTO {listing_table} (obj_path, obj_size) VALUES {batch_values}",
        format_=None,
    )
