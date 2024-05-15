import os

import xmltodict
from click import ClickException

from ch_tools.chadmin.cli.data_store_group import (
    CLICKHOUSE_DATA_PATH,
    CLICKHOUSE_METADATA_PATH,
    S3_METADATA_STORE_PATH,
    remove_from_ch_disk,
    remove_from_disk,
)
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.clickhouse.config import ClickhouseConfig


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


def _get_uuid_table(database_name: str, table_name: str) -> str:
    table_uuid = ""

    table_metadata_path = (
        CLICKHOUSE_METADATA_PATH + "/" + database_name + "/" + table_name + ".sql"
    )

    with open(table_metadata_path, "r") as metadata_file:
        for line in metadata_file:
            if "UUID" in line:
                parts = line.split()
                try:
                    uuid_index = parts.index("UUID")
                    # check size
                    table_uuid = parts[uuid_index + 1].strip("'")
                except ValueError:
                    raise RuntimeError(
                        f"Failed parse UUID for '{database_name}'.'{table_name}'"
                    )
                break
        else:
            raise RuntimeError(f"No UUID for '{database_name}'.'{table_name}'")

    # check is_valid_uuid
    return table_uuid


def delete_detached_table(
    ctx,
    database_name,
    table_name,
    echo=False,
):
    query = """
        SELECT
            count(uuid) AS count_tables
        FROM system.tables
        WHERE database = '{{ database_name }}'
          AND table = '{{ table_name }}'
    """
    result = execute_query(
        ctx,
        query,
        database_name=database_name,
        table_name=table_name,
        echo=echo,
        format_="JSON",
    )

    logging.info("delete_detached_table #1")
    assert 1 == len(result["data"])

    if result["data"][0].get("count_tables", "0") != "0":
        raise RuntimeError(
            f"Table '{database_name}'.'{table_name}' is attach. Use delete without --detach flag."
        )

    local_metadata_table_path = (
        CLICKHOUSE_METADATA_PATH + "/" + database_name + "/" + table_name + ".sql"
    )

    if not os.path.exists(local_metadata_table_path):
        raise RuntimeError(
            f"No metadata file for table '{database_name}'.'{table_name}'."
        )

    link_to_local_data = CLICKHOUSE_DATA_PATH + "/" + database_name + "/" + table_name
    # remove data from local storage
    local_table_data_path = link_to_local_data + "/"
    remove_from_disk(local_table_data_path)

    disk = "object_storage"
    table_uuid = _get_uuid_table(database_name, table_name)
    object_storage_table_data_path = (
        S3_METADATA_STORE_PATH + "/" + table_uuid[:3] + "/" + table_uuid
    )

    # remove data from object storage
    # @TODO copy paste
    disk_config = ClickhouseConfig.load().storage_configuration.get_disk_config(disk)

    disk_config_path = "/tmp/chadmin-ch-disks.xml"
    with open(disk_config_path, "w", encoding="utf-8") as f:
        xmltodict.unparse(
            {
                "yandex": {
                    "storage_configuration": {"disks": {disk: disk_config}},
                }
            },
            f,
            pretty=True,
        )

    code, stderr = remove_from_ch_disk(
        disk=disk,
        path=object_storage_table_data_path,
        disk_config_path=disk_config_path,
    )
    if code:
        raise RuntimeError(
            f"clickhouse-disks remove command has failed: retcode {code}, stderr: {stderr.decode()}"
        )

    # remove link to local data
    remove_from_disk(link_to_local_data)

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
