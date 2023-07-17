import logging
import socket
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
        host=socket.getfqdn(),
        port=None,
        user="_admin",
        settings=None,
        timeout=None,
        insecure=False,
    ):
        if port is None:
            port = 8443
        if timeout is None:
            timeout = 60
        if settings is None:
            settings = {}

        self._session = self._create_session(user=user, insecure=insecure)
        self._url = f"https://{host}:{port}"
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

        timeout = max(self._timeout, timeout or 0)

        logging.debug("Executing query: %s", query)
        try:
            response = self._session.post(
                self._url,
                params={
                    **self._settings,
                    "query": query,
                },
                json=post_data,
                timeout=timeout,
            )

            response.raise_for_status()

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
    def _create_session(user, insecure):
        session = requests.Session()

        session.verify = False if insecure else "/etc/clickhouse-server/ssl/allCAs.pem"

        if user:
            session.headers["X-ClickHouse-User"] = user

        return session
