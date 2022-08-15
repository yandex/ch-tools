from cloud.mdb.clickhouse.tools.chadmin.internal.utils import execute_query
from click import ClickException


def get_table(ctx, database, table, active_parts=None):
    tables = get_tables(ctx, database=database, table=table, active_parts=active_parts, verbose=True)

    if not table:
        raise ClickException(f'Table `{database}`.`{table}` not found.')

    return tables[0]


def get_tables(
    ctx, *, database=None, table=None, exclude_table=None, engine=None, active_parts=None, verbose=None, limit=None
):
    query = """
        SELECT
            database,
            table,
            disk_size,
            partitions,
            parts,
            rows,
            metadata_mtime,
        {% if verbose %}
            engine,
            create_table_query
        {% else %}
            engine
        {% endif %}
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
                 formatReadableSize(sum(bytes_on_disk)) "disk_size"
             FROM system.parts
        {% if active_parts %}
             WHERE active
        {% endif %}
             GROUP BY database, table
        ) parts USING database, table
        {% if database %}
        WHERE database {{ format_str_match(database) }}
        {% else %}
        WHERE database != 'system'
        {% endif %}
        {% if table %}
          AND table {{ format_str_match(table) }}
        {% endif %}
        {% if exclude_table %}
          AND table NOT {{ format_str_match(exclude_table) }}
        {% endif %}
        {% if engine %}
          AND engine {{ format_str_match(engine) }}
        {% endif %}
        ORDER BY database, table
        {% if limit is not none %}
        LIMIT {{ limit }}
        {% endif %}
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
        limit=limit,
        format='JSON',
    )['data']


def detach_table(ctx, database, table, *, cluster=None, echo=False, dry_run=False):
    """
    Perform "DETACH TABLE" for the specified table.
    """
    query = """
        DETACH TABLE `{{ database }}`.`{{ table }}`
        {% if cluster %}
        ON CLUSTER '{{ cluster }}'
        {% endif %}
        NO DELAY
        """
    execute_query(ctx, query, database=database, table=table, cluster=cluster, echo=echo, dry_run=dry_run, format=None)


def attach_table(ctx, database, table, *, cluster=None, echo=False, dry_run=False):
    """
    Perform "ATTACH TABLE" for the specified table.
    """
    query = """
        ATTACH TABLE `{{ database }}`.`{{ table }}`
        {% if cluster %}
        ON CLUSTER '{{ cluster }}'
        {% endif %}
        NO DELAY
        """
    execute_query(ctx, query, database=database, table=table, cluster=cluster, echo=echo, dry_run=dry_run, format=None)
