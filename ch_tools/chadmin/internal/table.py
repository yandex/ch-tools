import os
import uuid

from click import ClickException, Context

from ch_tools.chadmin.cli.data_store_group import (
    CLICKHOUSE_DATA_PATH,
    CLICKHOUSE_METADATA_PATH,
    make_ch_disks_config,
    remove_from_ch_disk,
    remove_from_disk,
)
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging


def get_table(ctx, database_name, table_name, active_parts=None):
    tables = list_tables(
        ctx,
        database_pattern=database_name,
        table_pattern=table_name,
        active_parts=active_parts,
    )

    if not tables:
        raise ClickException(f"Table `{database_name}`.`{table_name}` not found.")

    return tables[0]


def list_tables(
    ctx,
    *,
    database_pattern=None,
    exclude_database_pattern=None,
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
        {% if database_pattern -%}
            WHERE t.database {{ format_str_match(database_pattern) }}
        {% else %}
            WHERE t.database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
        {% endif -%}
        {% if exclude_database_pattern -%}
            AND t.database NOT {{ format_str_match(exclude_database_pattern) }}
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
        database_pattern=database_pattern,
        exclude_database_pattern=exclude_database_pattern,
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


def _is_valid_uuid(uuid_str):
    try:
        val = uuid.UUID(uuid_str)
    except ValueError:
        return False
    return str(val) == uuid_str


def _get_uuid_table(table_metadata_path: str) -> str:
    table_uuid = ""

    with open(table_metadata_path, "r", encoding="utf-8") as metadata_file:
        for line in metadata_file:
            if "UUID" in line:
                parts = line.split()
                try:
                    uuid_index = parts.index("UUID")
                    # check size
                    table_uuid = parts[uuid_index + 1].strip("'")
                except ValueError:
                    raise RuntimeError(
                        f"Failed parse UUID from '{table_metadata_path}'"
                    )
                break
        else:
            raise RuntimeError(f"No UUID in '{table_metadata_path}'")

    assert _is_valid_uuid(table_uuid)

    return table_uuid


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

    if 0 != len(response):
        raise RuntimeError(
            f"Table '{database_name}'.'{table_name}' is attach. Use delete without --detach flag."
        )


def _has_object_storage(ctx: Context) -> bool:
    query = """
        SELECT 1 FROM system.disks WHERE name = 'object_storage'
    """
    response = execute_query(
        ctx,
        query,
        format_="JSON",
    )["data"]

    if 0 == len(response):
        logging.info("No object storage.")
        return False

    logging.info("There is an object storage.")
    return True


def _remove_table_data_from_object_storage(local_metadata_table_path):
    table_uuid = _get_uuid_table(local_metadata_table_path)

    object_storage_table_data_path = "store" + "/" + table_uuid[:3] + "/" + table_uuid

    logging.info(
        "Table has UUID {}, path in S3: {}.", table_uuid, object_storage_table_data_path
    )

    disk = "object_storage"
    disk_config_path = make_ch_disks_config(disk)

    code, stderr = remove_from_ch_disk(
        disk=disk,
        path=object_storage_table_data_path,
        disk_config_path=disk_config_path,
    )
    if code:
        raise RuntimeError(
            f"clickhouse-disks remove command has failed: retcode {code}, stderr: {stderr.decode()}"
        )


def delete_detached_table(ctx, database_name, table_name):
    check_table_dettached(ctx, database_name, table_name)

    local_metadata_table_path = (
        CLICKHOUSE_METADATA_PATH + "/" + database_name + "/" + table_name + ".sql"
    )

    if not os.path.exists(local_metadata_table_path):
        raise RuntimeError(
            f"No metadata file for table '{database_name}'.'{table_name}' by path {local_metadata_table_path}."
        )

    link_to_local_data = CLICKHOUSE_DATA_PATH + "/" + database_name + "/" + table_name

    # remove data from local storage
    local_table_data_dir = link_to_local_data + "/"
    remove_from_disk(local_table_data_dir)

    if _has_object_storage(ctx):
        _remove_table_data_from_object_storage(local_metadata_table_path)

    # remove link to local data
    remove_from_disk(link_to_local_data)

    permanently_flag = local_metadata_table_path + ".detached"
    if os.path.exists(permanently_flag):
        remove_from_disk(permanently_flag)

    # remove table metadata file
    remove_from_disk(local_metadata_table_path)

    logging.info("Detached table {}.{} deleted.", database_name, table_name)


def materialize_ttl(ctx, database_name, table_name, echo=False, dry_run=False):
    """
    Materialize TTL for the specified table.
    """
    timeout = ctx.obj["config"]["clickhouse"]["alter_table_timeout"]
    query = f"ALTER TABLE `{database_name}`.`{table_name}` MATERIALIZE TTL"
    execute_query(ctx, query, timeout=timeout, echo=echo, dry_run=dry_run, format_=None)
