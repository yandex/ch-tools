"""
ClickHouse client.
"""

from http import HTTPStatus
from typing import Any, List, Optional, Sequence, Tuple

from hamcrest import assert_that
from requests import HTTPError
from requests.exceptions import ChunkedEncodingError

from ch_tools.chadmin.internal.clickhouse_disks import (
    CLICKHOUSE_PATH,
    OBJECT_STORAGE_DISK_TYPES,
    S3_PATH,
)
from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    S3ObjectLocalMetaData,
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
        return HTTPStatus.OK, str(execute_query(context, node, query=query))
    except HTTPError as e:
        return e.response.status_code, e.response.text
    except ClickhouseError as e:
        return e.response.status_code, e.response.text
    except ChunkedEncodingError as ex:
        # Related PR: https://github.com/ClickHouse/ClickHouse/pull/68800
        logging.warning(f"query={query} was caused the exception from server {ex}")
        return HTTPStatus.INTERNAL_SERVER_ERROR, "Workaround for in-progress op"


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


def _get_table_uuid(context: ContextT, table: str) -> str:
    table_uuid = context.uuid_to_table.get(table, None)
    assert_that(table_uuid is not None, f"not found saved uuid for table {table}")
    assert_that(len(table_uuid) > 0, f"found empty uuid for table {table}")
    return table_uuid


def check_table_exists_in_uuid_dir(
    context: ContextT, table: str, disk: str, node: str
) -> bool:
    table_uuid = _get_table_uuid(context, table)

    if disk in OBJECT_STORAGE_DISK_TYPES:
        path = S3_PATH
    else:
        path = CLICKHOUSE_PATH

    container = docker.get_container(context, node)

    table_path = path + "/store/" + table_uuid[:3] + "/" + table_uuid
    result = container.exec_run(["bash", "-c", f"ls {table_path}"], user="root")
    return True if 0 == result.exit_code else False


# ---------------------------------------------------------------------------
# Part recovery helpers
# ---------------------------------------------------------------------------


def get_part_path(context: ContextT, node: str, db: str, table: str) -> str:
    """Return the filesystem path of the first active part of *table* in *db*.

    Uses ``system.parts`` to find the path, then strips the trailing slash.
    """
    query = (
        f"SELECT path FROM system.parts "
        f"WHERE database='{db}' AND table='{table}' AND active=1 "
        f"ORDER BY name LIMIT 1 FORMAT TabSeparated"
    )
    result = execute_query(context, node, query)
    path = str(result).strip().rstrip("/")
    assert path, f"No active parts found for {db}.{table} on {node}"
    return path


def detach_part_copy_as_broken(context: ContextT, node: str, part_path: str) -> str:
    """Copy an active part directory into the table's *detached/* folder with a
    ``broken_`` prefix and return the path to the broken copy.

    *part_path* is the absolute path returned by :func:`get_part_path`.
    """
    import posixpath

    part_name = posixpath.basename(part_path)
    table_dir = posixpath.dirname(part_path)
    detached_dir = posixpath.join(table_dir, "detached")
    broken_name = f"broken_{part_name}"
    broken_path = posixpath.join(detached_dir, broken_name)

    container = docker.get_container(context, node)

    # Ensure detached/ exists
    result = container.exec_run(["bash", "-c", f"mkdir -p {detached_dir}"], user="root")
    assert result.exit_code == 0, f"mkdir detached failed: {result.output.decode()}"

    # Copy the active part
    result = container.exec_run(
        ["bash", "-c", f"cp -r {part_path} {broken_path}"], user="root"
    )
    assert result.exit_code == 0, f"cp part failed: {result.output.decode()}"

    return broken_path


def list_column_files(
    context: ContextT, node: str, part_path: str, column: str
) -> List[str]:
    """Return a list of absolute file paths inside *part_path* that belong to
    *column* (i.e. whose basename starts with ``<column>.`` or ``<column>_``).
    """
    container = docker.get_container(context, node)
    result = container.exec_run(["bash", "-c", f"ls {part_path}/"], user="root")
    assert result.exit_code == 0, f"ls part dir failed: {result.output.decode()}"

    files = []
    for name in result.output.decode().split():
        name = name.strip()
        if name.startswith(f"{column}.") or name.startswith(f"{column}_"):
            files.append(f"{part_path}/{name}")
    return files


def read_s3_metadata_keys(
    context: ContextT, node: str, file_path: str, s3_prefix: str
) -> List[str]:
    """Read an S3 metadata file from the container and return the list of full
    S3 object keys referenced by it.

    *s3_prefix* is the path prefix to prepend for metadata versions < 5
    (e.g. ``"data/cid1/shard_1/"``).
    """
    container = docker.get_container(context, node)
    result = container.exec_run(["bash", "-c", f"cat {file_path}"], user="root")
    if result.exit_code != 0:
        return []

    content = result.output.decode("latin-1")
    try:
        meta = S3ObjectLocalMetaData.from_string(content)
    except (ValueError, IndexError):
        # Not a metadata file (plain text like columns.txt)
        return []

    keys: List[str] = []
    for obj in meta.objects:
        if obj.key_is_full:
            keys.append(obj.key)
        else:
            keys.append(s3_prefix.rstrip("/") + "/" + obj.key)
    return keys
