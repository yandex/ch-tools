from click import ClickException

from ch_tools.chadmin.internal.utils import execute_query


def get_table(ctx, database, table, active_parts=None):
    tables = list_tables(
        ctx, database=database, table=table, active_parts=active_parts, verbose=True
    )

    if not tables:
        raise ClickException(f"Table `{database}`.`{table}` not found.")

    return tables[0]


def list_tables(
    ctx,
    *,
    database=None,
    table=None,
    exclude_table=None,
    engine=None,
    active_parts=None,
    verbose=None,
    order_by=None,
    limit=None,
):
    order_by = {
        "size": "bytes_on_disk DESC",
        "parts": "parts DESC",
        "rows": "rows DESC",
        None: "database, table",
    }[order_by]
    query = """
        SELECT
            database,
            table,
            formatReadableSize(bytes_on_disk) "disk_size",
            partitions,
            parts,
            rows,
            metadata_mtime,
            {%- if verbose %}
            engine,
            create_table_query
            {%- else %}
            engine
            {%- endif %}
        FROM (
            SELECT
                database,
                name "table",
                metadata_modification_time "metadata_mtime",
                engine,
                create_table_query
             FROM system.tables
        ) tables
        ALL LEFT JOIN (
             SELECT
                 database,
                 table,
                 uniq(partition) "partitions",
                 count() "parts",
                 sum(rows) "rows",
                 sum(bytes_on_disk) "bytes_on_disk"
             FROM system.parts
        {% if active_parts -%}
             WHERE active
        {% endif -%}
             GROUP BY database, table
        ) parts USING database, table
        {% if database -%}
        WHERE database {{ format_str_match(database) }}
        {% else -%}
        WHERE database NOT IN ('system', 'INFORMATION_SCHEMA')
        {% endif -%}
        {% if table -%}
          AND table {{ format_str_match(table) }}
        {% endif -%}
        {% if exclude_table -%}
          AND table NOT {{ format_str_match(exclude_table) }}
        {% endif -%}
        {% if engine -%}
          AND engine {{ format_str_match(engine) }}
        {% endif -%}
        ORDER BY {{ order_by }}
        {% if limit is not none -%}
        LIMIT {{ limit }}
        {% endif -%}
        """
    return execute_query(
        ctx,
        query,
        database=database,
        table=table,
        exclude_table=exclude_table,
        engine=engine,
        active_parts=active_parts,
        verbose=verbose,
        order_by=order_by,
        limit=limit,
        format_="JSON",
    )["data"]


def detach_table(ctx, database, table, *, cluster=None, echo=False, dry_run=False):
    """
    Perform "DETACH TABLE" for the specified table.
    """
    query = """
        DETACH TABLE `{{ database }}`.`{{ table }}`
        {%- if cluster %}
        ON CLUSTER '{{ cluster }}'
        {%- endif %}
        NO DELAY
        """
    execute_query(
        ctx,
        query,
        database=database,
        table=table,
        cluster=cluster,
        echo=echo,
        dry_run=dry_run,
        format_=None,
    )


def attach_table(ctx, database, table, *, cluster=None, echo=False, dry_run=False):
    """
    Perform "ATTACH TABLE" for the specified table.
    """
    query = """
        ATTACH TABLE `{{ database }}`.`{{ table }}`
        {%- if cluster %}
        ON CLUSTER '{{ cluster }}'
        {%- endif %}
        """
    execute_query(
        ctx,
        query,
        database=database,
        table=table,
        cluster=cluster,
        echo=echo,
        dry_run=dry_run,
        format_=None,
    )


def delete_table(ctx, database, table, *, cluster=None, echo=False, dry_run=False):
    """
    Perform "DROP TABLE" for the specified table.
    """
    query = """
        DROP TABLE `{{ database }}`.`{{ table }}`
        {%- if cluster %}
        ON CLUSTER '{{ cluster }}'
        {%- endif %}
        NO DELAY
        """
    execute_query(
        ctx,
        query,
        database=database,
        table=table,
        cluster=cluster,
        echo=echo,
        dry_run=dry_run,
        format_=None,
    )


def materialize_ttl(ctx, database, table, echo=False, dry_run=False):
    """
    Materialize TTL for the specified table.
    """
    query = f"ALTER TABLE `{database}`.`{table}` MATERIALIZE TTL"
    execute_query(ctx, query, timeout=300, echo=echo, dry_run=dry_run, format_=None)
