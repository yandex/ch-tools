import os

from click import ClickException, Context

from ch_tools.chadmin.cli.table_metadata import TableMetadata
from ch_tools.chadmin.internal.clickhouse_disks import (
    CLICKHOUSE_DATA_PATH,
    CLICKHOUSE_METADATA_PATH,
    CLICKHOUSE_PATH,
    S3_PATH,
    make_ch_disks_config,
    remove_from_ch_disk,
)
from ch_tools.chadmin.internal.utils import execute_query, remove_from_disk
from ch_tools.common import logging

DISK_LOCAL_KEY = "local"
DISK_OBJECT_STORAGE_KEY = "object_storage"


def get_table(ctx, database_name, table_name, active_parts=None):
    tables = list_tables(
        ctx,
        database_name=database_name,
        table_name=table_name,
        active_parts=active_parts,
    )

    if not tables:
        raise ClickException(f"Table `{database_name}`.`{table_name}` not found.")

    return tables[0]


def list_tables(
    ctx,
    *,
    database_name=None,
    database_pattern=None,
    exclude_database_pattern=None,
    table_name=None,
    table_pattern=None,
    exclude_table_pattern=None,
    engine_pattern=None,
    exclude_engine_pattern=None,
    is_readonly=None,
    active_parts=None,
    order_by=None,
    limit=None,
):
    order_by = {
        "size": "disk_size DESC",
        "parts": "parts DESC",
        "rows": "rows DESC",
        None: "database, name",
    }[order_by]
    query = """
        WITH tables AS (
            SELECT
                t.database,
                t.name,
                t.metadata_modification_time,
                t.engine,
                t.data_paths,
                t.create_table_query
            FROM system.tables t
        {% if is_readonly -%}
            LEFT JOIN system.replicas r ON r.database = t.database AND r.table = t.name
        {% endif -%}
            WHERE true
        {% if database_name -%}
            AND t.database = '{{ database_name }}'
        {% endif -%}
        {% if database_pattern -%}
            AND t.database {{ format_str_match(database_pattern) }}
        {% endif -%}
        {% if not database_name and not database_pattern  -%}
            AND t.database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
        {% endif -%}
        {% if exclude_database_pattern -%}
            AND t.database NOT {{ format_str_match(exclude_database_pattern) }}
        {% endif -%}
        {% if table_name -%}
            AND t.name = '{{ table_name }}'
        {% endif -%}
        {% if table_pattern -%}
            AND t.name {{ format_str_match(table_pattern) }}
        {% endif -%}
        {% if exclude_table_pattern -%}
            AND t.name NOT {{ format_str_match(exclude_table_pattern) }}
        {% endif -%}
        {% if engine_pattern -%}
            AND t.engine {{ format_str_match(engine_pattern) }}
        {% endif -%}
        {% if exclude_engine_pattern -%}
           AND t.engine NOT {{ format_str_match(exclude_engine_pattern) }}
        {% endif -%}
        {% if is_readonly -%}
           AND r.is_readonly
        {% endif -%}
        ),
        parts AS (
            SELECT
                p.database,
                p.table,
                uniq(p.partition) "partitions",
                count() "parts",
                sum(p.rows) "rows",
                sum(p.bytes_on_disk) "disk_size"
            FROM system.parts p
            JOIN tables t ON t.database = p.database AND t.name = p.table
        {% if active_parts -%}
            WHERE p.active
        {% endif -%}
            GROUP BY p.database, p.table
        )
        SELECT
            t.database,
            t.name,
            t.engine,
            t.create_table_query,
            t.metadata_modification_time,
            t.data_paths,
            p.disk_size,
            p.partitions,
            p.parts,
            p.rows
        FROM tables t
        LEFT JOIN parts p ON p.database = t.database AND p.table = t.name
        ORDER BY {{ order_by }}
        {% if limit is not none -%}
        LIMIT {{ limit }}
        {% endif -%}
        """
    return execute_query(
        ctx,
        query,
        database_name=database_name,
        database_pattern=database_pattern,
        exclude_database_pattern=exclude_database_pattern,
        table_name=table_name,
        table_pattern=table_pattern,
        exclude_table_pattern=exclude_table_pattern,
        engine_pattern=engine_pattern,
        exclude_engine_pattern=exclude_engine_pattern,
        is_readonly=is_readonly,
        active_parts=active_parts,
        order_by=order_by,
        limit=limit,
        format_="JSON",
    )["data"]


def list_table_columns(ctx, database_name, table_name):
    query = """
        SELECT
            name,
            type,
            default_kind,
            default_expression,
            data_compressed_bytes "disk_size",
            data_uncompressed_bytes "uncompressed_size",
            marks_bytes
        FROM system.columns
        WHERE database = '{{ database_name }}'
          AND table = '{{ table_name }}'
        """
    return execute_query(
        ctx,
        query,
        database_name=database_name,
        table_name=table_name,
        format_="JSON",
    )["data"]


def detach_table(
    ctx,
    database_name,
    table_name,
    *,
    cluster=None,
    echo=False,
    dry_run=False,
):
    """
    Perform "DETACH TABLE" for the specified table.
    """
    logging.info("Detaching table `{}`.`{}`", database_name, table_name)
    timeout = ctx.obj["config"]["clickhouse"]["detach_table_timeout"]
    query = """
        DETACH TABLE `{{ database_name }}`.`{{ table_name }}`
        {%- if cluster %}
        ON CLUSTER '{{ cluster }}'
        {%- endif %}
        NO DELAY
        """
    execute_query(
        ctx,
        query,
        timeout=timeout,
        database_name=database_name,
        table_name=table_name,
        cluster=cluster,
        echo=echo,
        dry_run=dry_run,
        format_=None,
    )


def attach_table(
    ctx,
    database_name,
    table_name,
    *,
    cluster=None,
    echo=False,
    dry_run=False,
):
    """
    Perform "ATTACH TABLE" for the specified table.
    """
    logging.info("Attaching table `{}`.`{}`", database_name, table_name)
    timeout = ctx.obj["config"]["clickhouse"]["attach_table_timeout"]
    query = """
        ATTACH TABLE `{{ database_name }}`.`{{ table_name }}`
        {%- if cluster %}
        ON CLUSTER '{{ cluster }}'
        {%- endif %}
        """
    execute_query(
        ctx,
        query,
        timeout=timeout,
        database_name=database_name,
        table_name=table_name,
        cluster=cluster,
        echo=echo,
        dry_run=dry_run,
        format_=None,
    )


def delete_table(
    ctx,
    database_name,
    table_name,
    *,
    cluster=None,
    echo=False,
    sync_mode=True,
    dry_run=False,
):
    """
    Perform "DROP TABLE" for the specified table.
    """
    logging.info("Deleting table `{}`.`{}`", database_name, table_name)
    timeout = ctx.obj["config"]["clickhouse"]["drop_table_timeout"]
    query = """
        DROP TABLE `{{ database_name }}`.`{{ table_name }}`
        {%- if cluster %}
        ON CLUSTER '{{ cluster }}'
        {%- endif %}
        {%- if sync_mode %}
        NO DELAY
        {%- endif %}
        """
    execute_query(
        ctx,
        query,
        timeout=timeout,
        database_name=database_name,
        table_name=table_name,
        cluster=cluster,
        sync_mode=sync_mode,
        echo=echo,
        dry_run=dry_run,
        format_=None,
    )


def check_table_dettached(ctx, database_name, table_name):
    query = """
        SELECT
            1
        FROM system.tables
        WHERE database = '{{ database_name }}'
          AND table = '{{ table_name }}'
    """

    response = execute_query(
        ctx,
        query,
        database_name=database_name,
        table_name=table_name,
        format_="JSON",
    )["data"]

    if len(response):
        raise RuntimeError(
            f"Table '{database_name}'.'{table_name}' is attached. Use delete without --detach flag."
        )


def check_table_engine_supported(engine: str) -> None:
    if "MergeTree" not in engine:
        raise RuntimeError(
            f"Doesn't support removing detached table with engine {engine}"
        )


def _get_disks_data(ctx: Context) -> dict:
    # Disk type 'cache' of disk object_storage_cache is not supported by clickhouse-disks
    query = """
        SELECT name, type FROM system.disks
        WHERE name!='object_storage_cache'
    """
    response = execute_query(
        ctx,
        query,
        format_="JSON",
    )["data"]

    logging.info("Found disks: {}", response)

    result = {}

    for data_disk in response:
        if data_disk.get("type") == "s3":
            result[DISK_OBJECT_STORAGE_KEY] = data_disk["name"]
        else:
            result[DISK_LOCAL_KEY] = data_disk["name"]

    logging.info("Table disks: {}", result)

    return result


def _is_should_use_ch_disk_remover(table_data_path: str, disk_type: str) -> bool:
    if disk_type == DISK_LOCAL_KEY:
        return os.path.exists(CLICKHOUSE_PATH + table_data_path)
    if disk_type == DISK_OBJECT_STORAGE_KEY:
        return os.path.exists(S3_PATH + table_data_path)

    return True


def _remove_table_data_from_disk(
    table_uuid: str, disk_name: str, disk_type: str
) -> None:
    logging.info(
        "_remove_table_data_from_disk: UUID={}, disk={}",
        table_uuid,
        disk_name,
    )

    table_data_path = "store" + "/" + table_uuid[:3] + "/" + table_uuid

    logging.info(
        "Table has UUID: {}, disk: {}, data path: {}.",
        table_uuid,
        disk_name,
        table_data_path,
    )

    disk_config_path = make_ch_disks_config(disk_name)

    if not _is_should_use_ch_disk_remover(table_data_path, disk_type):
        logging.warning(
            f"Dir {table_data_path} doesn't exist on disk {disk_name}. Skip launch clickhouse-disks for Clickhouse 22.8."
        )

    code, stderr = remove_from_ch_disk(
        disk=disk_name,
        path=table_data_path,
        disk_config_path=disk_config_path,
    )
    if code:
        raise RuntimeError(
            f"clickhouse-disks remove command has failed: retcode {code}, stderr: {stderr.decode()}"
        )


def delete_detached_table(ctx, database_name, table_name):
    logging.info("Call delete_detached_table: {}.{}", database_name, table_name)

    escaped_database_name = database_name.encode("unicode_escape").decode("utf-8")
    escaped_table_name = table_name.encode("unicode_escape").decode("utf-8")

    logging.info("Escaped params: {}.{}", escaped_database_name, escaped_table_name)

    check_table_dettached(ctx, escaped_database_name, escaped_table_name)

    local_metadata_table_path = (
        CLICKHOUSE_METADATA_PATH
        + "/"
        + escaped_database_name
        + "/"
        + escaped_table_name
        + ".sql"
    )

    if not os.path.exists(local_metadata_table_path):
        raise RuntimeError(
            f"No metadata file for table '{escaped_database_name}'.'{escaped_table_name}' by path {local_metadata_table_path}."
        )

    table_metadata = TableMetadata(local_metadata_table_path)
    check_table_engine_supported(engine=table_metadata.table_engine)

    for disk_type, disk_name in _get_disks_data(ctx).items():
        _remove_table_data_from_disk(
            table_uuid=table_metadata.table_uuid,
            disk_name=disk_name,
            disk_type=disk_type,
        )

    link_to_local_data = (
        CLICKHOUSE_DATA_PATH + "/" + escaped_database_name + "/" + escaped_table_name
    )
    logging.info("Remove link: {}", link_to_local_data)
    remove_from_disk(link_to_local_data)

    permanently_flag = local_metadata_table_path + ".detached"
    if os.path.exists(permanently_flag):
        logging.info("Remove permanently flag: {}", permanently_flag)
        remove_from_disk(permanently_flag)

    logging.info("Remove table metadata: {}", local_metadata_table_path)
    remove_from_disk(local_metadata_table_path)

    logging.info(
        "Detached table {}.{} deleted.", escaped_database_name, escaped_table_name
    )


def materialize_ttl(ctx, database_name, table_name, echo=False, dry_run=False):
    """
    Materialize TTL for the specified table.
    """
    timeout = ctx.obj["config"]["clickhouse"]["alter_table_timeout"]
    query = f"ALTER TABLE `{database_name}`.`{table_name}` MATERIALIZE TTL"
    execute_query(ctx, query, timeout=timeout, echo=echo, dry_run=dry_run, format_=None)
