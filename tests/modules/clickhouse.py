"""
ClickHouse client.
"""

from typing import Any, Optional, Sequence, Tuple

from hamcrest import assert_that
from requests import HTTPError
from requests.exceptions import ChunkedEncodingError

from ch_tools.chadmin.internal.clickhouse_disks import (
    CLICKHOUSE_PATH,
    OBJECT_STORAGE_DISK_TYPES,
    S3_PATH,
)
from ch_tools.common import logging
from ch_tools.common.clickhouse.client.clickhouse_client import (
    ClickhouseClient,
    ClickhousePort,
)
from ch_tools.common.clickhouse.client.error import ClickhouseError

from . import docker
from .typing import ContextT


def clickhouse_client(context: ContextT, node_name: str) -> ClickhouseClient:
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


def ping(context: ContextT, node: str) -> None:
    """
    Ping ClickHouse server.
    """
    return execute_query(context, node, query=None)


def get_response(context: ContextT, node: str, query: str) -> Tuple[int, str]:
    """
    Execute arbitrary query and return result
    """
    try:
        return 200, str(execute_query(context, node, query=query))
    except HTTPError as e:
        return e.response.status_code, e.response.text
    except ClickhouseError as e:
        return e.response.status_code, e.response.text
    except ChunkedEncodingError as ex:
        # Related PR: https://github.com/ClickHouse/ClickHouse/pull/68800
        logging.warning(f"query={query} was caused the exception from server {ex}")
        return 500, "Workaround for in-progress op"


def get_version(context: ContextT, node: str) -> dict:
    """
    Get ClickHouse version.
    """
    return execute_query(context, node, "SELECT version()", format_="JSONCompact")


def get_all_user_data(context: ContextT, node: str) -> Tuple[int, dict]:
    """
    Retrieve all user data.
    """
    user_data = {}
    rows_count = 0
    for db_name, table_name, columns in _get_all_user_tables(context, node):
        query = f"""
            SELECT *
            FROM `{db_name}`.`{table_name}`
            ORDER BY {','.join(columns)}
            """
        table_data = execute_query(context, node, query, format_="JSONCompact")
        user_data[".".join([db_name, table_name])] = table_data["data"]
        rows_count += table_data["rows"]
    return rows_count, user_data


def get_all_user_schemas(context: ContextT, node: str) -> dict:
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


def get_all_user_databases(context: ContextT, node: str) -> Sequence[str]:
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


def drop_database(context: ContextT, node: str, db_name: str) -> None:
    """
    Drop database.
    """
    execute_query(context, node, f"DROP DATABASE {db_name}")


def _get_all_user_tables(context: ContextT, node: str) -> dict:
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
    context: ContextT,
    node: str,
    query: Optional[str] = None,
    format_: Optional[str] = None,
) -> Any:

    client = clickhouse_client(context, node)

    try:
        response = client.query(query, format_=format_)
    except HTTPError as e:
        logging.critical(f"Error while performing request: {e.response.text}")
        raise

    return response


def _get_table_uuid(context, table):
    table_uuid = context.uuid_to_table.get(table, None)
    assert_that(table_uuid is not None, f"not found saved uuid for table {table}")
    assert_that(len(table_uuid) > 0, f"found empty uuid for table {table}")
    return table_uuid


def check_table_exists_in_uuid_dir(context, table, disk, node):
    table_uuid = _get_table_uuid(context, table)

    if disk in OBJECT_STORAGE_DISK_TYPES:
        path = S3_PATH
    else:
        path = CLICKHOUSE_PATH

    container = docker.get_container(context, node)

    table_path = path + "/store/" + table_uuid[:3] + "/" + table_uuid
    result = container.exec_run(["bash", "-c", f"ls {table_path}"], user="root")
    return True if 0 == result.exit_code else False
