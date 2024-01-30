import logging
from datetime import timedelta
from typing import Any, Dict, Optional

import requests
from jinja2 import Environment
from typing_extensions import Self

from ch_tools.common.utils import version_ge

from .error import ClickhouseError
from .retry import retry
from .utils import _format_str_imatch, _format_str_match


class ClickhouseClient:
    """
    ClickHouse client wrapper.
    """

    def __init__(
        self,
        *,
        host,
        protocol,
        insecure,
        port,
        user,
        password,
        timeout,
        settings,
    ):
        self._session = self._create_session(
            user=user, password=password, insecure=insecure
        )
        self._url = f"{protocol}://{host}:{port}"
        self._settings = settings
        self._timeout = timeout
        self._ch_version = None

    def get_clickhouse_version(self):
        """
        Get ClickHouse server version.
        """
        if self._ch_version is None:
            self._ch_version = self.query("SELECT version()")

        return self._ch_version

    def get_uptime(self):
        """
        Get uptime of ClickHouse server.
        """
        seconds = int(self.query("SELECT uptime()"))
        return timedelta(seconds=seconds)

    @retry(requests.exceptions.ConnectionError)
    def query(
        self: Self,
        query: str,
        query_args: Optional[Dict[str, Any]] = None,
        format_: Optional[str] = None,
        post_data: Any = None,
        timeout: Optional[int] = None,
        echo: bool = False,
        dry_run: bool = False,
        stream: bool = False,
        settings: Optional[dict] = None,
    ) -> Any:
        """
        Execute query.
        """
        if query_args:
            query = self.render_query(query, **query_args)

        if format_:
            query += f" FORMAT {format_}"

        if echo:
            print(query, "\n")

        if dry_run:
            return None

        if timeout:
            timeout = timeout
        else:
            timeout = self._timeout
        per_query_settings = settings or {}

        logging.debug("Executing query: %s", query)
        try:
            response = self._session.post(
                self._url,
                params={
                    **self._settings,
                    "query": query,
                    **per_query_settings,  # overwrites previous settings
                },
                json=post_data,
                timeout=timeout,
                stream=stream,
            )

            response.raise_for_status()

            # Return response for iterating over
            if stream:
                return response

            if format_ in ("JSON", "JSONCompact"):
                return response.json()

            return response.text.strip()
        except requests.exceptions.HTTPError as e:
            raise ClickhouseError(query, e.response) from None

    def render_query(self, query, **kwargs):
        env = Environment()

        env.globals["version_ge"] = lambda version: version_ge(
            self.get_clickhouse_version(), version
        )
        env.globals["format_str_match"] = _format_str_match
        env.globals["format_str_imatch"] = _format_str_imatch

        template = env.from_string(query)
        return template.render(kwargs)

    @staticmethod
    def _create_session(user, password, insecure):
        session = requests.Session()

        session.verify = False if insecure else "/etc/clickhouse-server/ssl/allCAs.pem"

        if user:
            session.headers["X-ClickHouse-User"] = user
        if password:
            session.headers["X-ClickHouse-Key"] = password

        return session


def clickhouse_client(ctx):
    """
    Return ClickHouse client from the context if it exists.
    Init ClickHouse client and store to the context if it doesn't exist.
    """
    if not ctx.obj.get("chcli"):
        config = ctx.obj["config"]["clickhouse"]
        user, password = clickhouse_credentials(ctx)
        ctx.obj["chcli"] = ClickhouseClient(
            host=config["host"],
            protocol=config["protocol"],
            insecure=config["insecure"],
            port=config["port"],
            user=user,
            password=password,
            timeout=config["timeout"],
            settings=config["settings"],
        )

    return ctx.obj["chcli"]


def clickhouse_credentials(ctx):
    """
    Return credentials to connect to ClickHouse.
    """
    config = ctx.obj["config"]["clickhouse"]

    user = config["user"]
    password = config["password"]
    if ctx.obj.get("monitoring", False) and config["monitoring_user"]:
        user = config["monitoring_user"]
        password = config["monitoring_password"]

    return user, password
