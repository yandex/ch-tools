"""
ClickHouse client.
"""

import logging
from typing import Any, Sequence, Tuple, Union
from urllib.parse import urljoin

from requests import HTTPError, Session

from . import docker
from .typing import ContextT


class ClickhouseClient:
    """
    ClickHouse Client.
    """

    def __init__(self, context: ContextT, node_name: str) -> None:
        protocol = "http"
        port = context.conf["services"]["clickhouse"]["expose"][protocol]
        host, exposed_port = docker.get_exposed_port(
            docker.get_container(context, node_name), port
        )

        self._session = Session()
        if getattr(context, "ch_user", None) is not None:
            self._session.headers["X-ClickHouse-User"] = context.ch_user
        self._url = f"{protocol}://{host}:{exposed_port}"

        self._timeout = 30

    def ping(self) -> None:
        """
        Ping ClickHouse server.
        """
        self._query("GET", url="ping", params={})

    def execute(self, query: str) -> None:
        """
        Execute arbitrary query.
        """
        self._query("POST", query=query)

    def get_response(self, query: str) -> Tuple[int, str]:
        """
        Execute arbitrary query and return result
        """
        try:
            return 200, str(self._query("POST", query=query))
        except HTTPError as e:
            return e.response.status_code, e.response.text

    def get_version(self) -> str:
        """
        Get ClickHouse version.
        """
        return self._query("GET", "SELECT version()")

    def get_all_user_data(self) -> Tuple[int, dict]:
        """
        Retrieve all user data.
        """
        user_data = {}
        rows_count = 0
        for db_name, table_name, columns in self._get_all_user_tables():
            query = f"""
                SELECT *
                FROM `{db_name}`.`{table_name}`
                ORDER BY {','.join(columns)}
                FORMAT JSONCompact
                """
            table_data = self._query("GET", query)
            user_data[".".join([db_name, table_name])] = table_data["data"]
            rows_count += table_data["rows"]
        return rows_count, user_data

    def get_all_user_schemas(self) -> dict:
        """
        Retrieve DDL for user schemas.
        """
        all_tables_desc = {}
        for db_name, table_name, _ in self._get_all_user_tables():
            query = f"""
                DESCRIBE `{db_name}`.`{table_name}`
                FORMAT JSONCompact
                """
            table_data = self._query("GET", query)
            all_tables_desc[(db_name, table_name)] = table_data["data"]
        return all_tables_desc

    def get_all_user_databases(self) -> Sequence[str]:
        """
        Get user databases.
        """
        query = """
            SELECT name
            FROM system.databases
            WHERE name NOT IN ('system')
            FORMAT JSONCompact
            """

        databases = self._query("GET", query)["data"]
        return [db[0] for db in databases]

    def drop_database(self, db_name: str) -> None:
        """
        Drop database.
        """
        self._query("POST", f"DROP DATABASE {db_name}")

    def _get_all_user_tables(self) -> dict:
        query = """
            SELECT
                database,
                table,
                groupArray(name) AS columns
            FROM system.columns
            WHERE database NOT IN ('system')
            GROUP BY database, table
            ORDER BY database, table
            FORMAT JSONCompact
            """
        return self._query("GET", query)["data"]

    def _query(
        self,
        method: str,
        query: str = None,
        url: str = None,
        params: dict = None,
        data: Union[bytes, str] = None,
    ) -> Any:
        if url:
            url = urljoin(self._url, url)
        else:
            url = self._url

        if isinstance(data, str):
            data = data.encode()

        if params is None:
            params = {
                "wait_end_of_query": 1,
            }

        if query:
            params["query"] = query

        try:
            logging.debug("Executing ClickHouse query: %s", query)
            response = self._session.request(
                method, url, params=params, data=data, timeout=self._timeout
            )

            response.raise_for_status()
        except HTTPError as e:
            logging.critical("Error while performing request: %s", e.response.text)
            raise

        try:
            return response.json()
        except ValueError:
            return str.strip(response.text)
