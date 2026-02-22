import click

from ch_tools.common.clickhouse.client.clickhouse_client import clickhouse_client
from ch_tools.common.result import CRIT, OK, Result


@click.command("ro-replica")
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Show details about ro tables.",
)
@click.option(
    "-d",
    "--database",
    "database_pattern",
    help="Check tables only in the databases that matches specified pattern.",
)
@click.pass_context
def ro_replica_command(
    ctx: click.Context,
    verbose: bool,
    database_pattern: str,
) -> Result:
    """
    Check for readonly replicated tables.
    """
    query = """
        SELECT database, table, replica_path, last_queue_update_exception, zookeeper_exception
        FROM system.replicas
        WHERE is_readonly
        {% if database_pattern -%}
          AND database {{ format_str_match(database_pattern) }}
        {% endif -%}
    """
    query_result = clickhouse_client(ctx).query_json_data(
        query=query,
        query_args={
            "database_pattern": database_pattern,
        },
        compact=False,
    )
    if not query_result:
        return Result(OK)

    verbose_msg = ""
    if verbose:
        headers = [
            "database",
            "table",
            "replica_path",
            "last_queue_update_exception",
            "zookeeper_exception",
        ]

        formatted_data = []
        for item in query_result:
            formatted_row = "\n".join(
                [f"{header}: {item[header]}" for header in headers if header in item]
            )
            formatted_data.append(formatted_row)

        verbose_msg = "\n\n".join(data for data in formatted_data)

    tables_str = ", ".join(
        f"{item['database']}.{item['table']}" for item in query_result
    )

    return Result(CRIT, f"Readonly replica tables: {tables_str}", verbose=verbose_msg)
