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
@click.pass_context
def ro_replica_command(ctx, verbose=False):
    """
    Check for readonly replicated tables.
    """
    query = """
        SELECT database, table, last_queue_update_exception, zookeeper_exception 
        FROM system.replicas WHERE is_readonly
    """
    response = clickhouse_client(ctx).query_json_data(query, compact=False)
    if response:
        msg_verbose = ""

        if verbose:
            headers = [
                "database",
                "table",
                "last_queue_update_exception",
                "zookeeper_exception",
            ]

            formatted_data = []

            for item in response:
                formatted_row = "\n".join(
                    [
                        f"{header}: {item[header]}"
                        for header in headers
                        if header in item
                    ]
                )
                formatted_data.append(formatted_row)

            msg_verbose = "\n\n".join(data for data in formatted_data)

        tables_str = ", ".join(
            f"{item['database']}.{item['table']}" for item in response
        )

        return Result(
            CRIT, f"Readonly replica tables: {tables_str}", verbose=msg_verbose
        )

    return Result(OK)
