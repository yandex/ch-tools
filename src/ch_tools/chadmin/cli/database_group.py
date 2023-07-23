from click import argument, group, option, pass_context

from ch_tools.chadmin.internal.utils import execute_query


@group("database")
def database_group():
    """Commands to manage databases."""
    pass


@database_group.command("get")
@argument("database")
@option(
    "--active",
    "--active-parts",
    "active_parts",
    is_flag=True,
    help="Account only active data parts.",
)
@pass_context
def get_database_command(ctx, database, active_parts):
    print(
        get_databases(
            ctx, database=database, active_parts=active_parts, format_="Vertical"
        )
    )


@database_group.command("list")
@option("-d", "--database")
@option("--exclude-database")
@option(
    "--active",
    "--active-parts",
    "active_parts",
    is_flag=True,
    help="Account only active data parts.",
)
@pass_context
def list_databases_command(ctx, **kwargs):
    print(get_databases(ctx, **kwargs, format_="PrettyCompact"))


@database_group.command("delete")
@pass_context
@option("-d", "--database")
@option("--exclude-database")
@option("-a", "--all", "all_", is_flag=True, help="Delete all databases.")
@option("--cluster")
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
def delete_databases_command(ctx, dry_run, all_, database, exclude_database, cluster):
    if not any((all_, database)):
        ctx.fail("At least one of --all, --database options must be specified.")

    for d in get_databases(
        ctx, database=database, exclude_database=exclude_database, format_="JSON"
    )["data"]:
        query = """
            DROP DATABASE `{{ database }}`
            {% if cluster %}
            ON CLUSTER '{{ cluster }}'
            {% endif %}
            """
        execute_query(
            ctx,
            query,
            database=d["database"],
            cluster=cluster,
            echo=True,
            dry_run=dry_run,
        )


def get_databases(
    ctx, database=None, exclude_database=None, active_parts=None, format_=None
):
    query = """
        SELECT
            database,
            engine,
            tables,
            formatReadableSize(bytes_on_disk) "disk_size",
            partitions,
            parts,
            rows
        FROM (
            SELECT
                name "database",
                engine
            FROM system.databases
        ) q1
        ALL LEFT JOIN (
            SELECT
                database,
                count() "tables",
                sum(bytes_on_disk) "bytes_on_disk",
                sum(partitions) "partitions",
                sum(parts) "parts",
                sum(rows) "rows"
            FROM (
                SELECT
                    database,
                    name "table"
                FROM system.tables
            ) q2_1
            ALL LEFT JOIN (
                SELECT
                    database,
                    table,
                    uniq(partition) "partitions",
                    count() "parts",
                    sum(rows) "rows",
                    sum(bytes_on_disk) "bytes_on_disk"
                FROM system.parts
        {% if active_parts %}
                WHERE active
        {% endif %}
                GROUP BY database, table
            ) q2_2
            USING database, table
            GROUP BY database
        ) q2
        USING database
        {% if database %}
        WHERE database {{ format_str_match(database) }}
        {% else %}
        WHERE database NOT IN ('system', 'INFORMATION_SCHEMA')
        {% endif %}
        {% if exclude_database %}
          AND database != '{{ exclude_database }}'
        {% endif %}
        ORDER BY database
        """
    return execute_query(
        ctx,
        query,
        database=database,
        exclude_database=exclude_database,
        active_parts=active_parts,
        format_=format_,
    )
