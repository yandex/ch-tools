"""
ClickHouse client.
"""

import logging
import sys
from typing import Any, Optional, Sequence, Tuple, Union
from urllib.parse import urljoin

from requests import HTTPError, Session

from . import docker
from .typing import ContextT

sys.path.insert(0, "/home/kirillgarbar/repos/ch-tools")

from ch_tools.common.clickhouse.client.clickhouse_client import (
    ClickhouseClient,
    ClickhousePort,
)
from ch_tools.common.clickhouse.client.error import ClickhouseError


def clickhouse_client(context: ContextT, node_name: str):
    protocol = "http"
    port = context.conf["services"]["clickhouse"]["expose"][protocol]
    host, port = docker.get_exposed_port(docker.get_container(context, node_name), port)

    user = None
    if getattr(context, "ch_user", None) is not None:
        user = context.ch_user

    return ClickhouseClient(
        host=host,
        insecure=True,
        user=user,
        ports={ClickhousePort.HTTP: port},
        timeout=30,
    )


def ping(context, node) -> None:
    """
    Ping ClickHouse server.
    """
    return execute_query(context, node, query=None)


def get_response(context, node, query: str) -> Tuple[int, str]:
    """
    Execute arbitrary query and return result
    """
    try:
        return 200, str(execute_query(context, node, query=query))
    except HTTPError as e:
        return e.response.status_code, e.response.text
    except ClickhouseError as e:
        print(e.response)
        print(e.query)
        return e.response.status_code, e.response.text


def get_version(context, node) -> str:
    """
    Get ClickHouse version.
    """
    return execute_query(context, node, "SELECT version()", format_="JSONCompact")


def get_all_user_data(context, node) -> Tuple[int, dict]:
    """
    Retrieve all user data.
    """
    user_data = {}
    rows_count = 0
    for db_name, table_name, columns in context, _get_all_user_tables(context, node):
        query = f"""
            SELECT *
            FROM `{db_name}`.`{table_name}`
            ORDER BY {','.join(columns)}
            """
        table_data = execute_query(context, node, query, format_="JSONCompact")
        user_data[".".join([db_name, table_name])] = table_data["data"]
        rows_count += table_data["rows"]
    return rows_count, user_data


def get_all_user_schemas(context, node) -> dict:
    """
    Retrieve DDL for user schemas.
    """
    all_tables_desc = {}
    for db_name, table_name, _ in _get_all_user_tables(context, node):
        query = f"""
            DESCRIBE `{db_name}`.`{table_name}`
            """
        table_data = execute_query(context, node, query, format_="JSONCompact")
        all_tables_desc[(db_name, table_name)] = table_data["data"]
    return all_tables_desc


def get_all_user_databases(context, node) -> Sequence[str]:
    """
    Get user databases.
    """
    query = """
        SELECT name
        FROM system.databases
        WHERE name NOT IN ('system')
        """

    databases = execute_query(context, node, query, format_="JSONCompact")["data"]
    return [db[0] for db in databases]


def drop_database(context, node, db_name: str) -> None:
    """
    Drop database.
    """
    execute_query(context, node, f"DROP DATABASE {db_name}")


def _get_all_user_tables(context, node) -> dict:
    query = """
        SELECT
            database,
            table,
            groupArray(name) AS columns
        FROM system.columns
        WHERE database NOT IN ('system')
        GROUP BY database, table
        ORDER BY database, table
        """
    return execute_query(context, node, query, format_="JSONCompact")["data"]


def execute_query(
    context,
    node,
    query: Optional[str] = None,
    data: Union[None, bytes, str] = None,
    format_=None,
) -> Any:

    client = clickhouse_client(context, node)

    if isinstance(data, str):
        data = data.encode()

    try:
        logging.debug("Executing ClickHouse query: %s", query)
        response = client.query(query, format_=format_)
    except HTTPError as e:
        logging.critical("Error while performing request: %s", e.response.text)
        raise

    return response
