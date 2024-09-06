import json
import subprocess
from datetime import timedelta
from typing import Any, Dict, Optional

import requests
from click import Context
from jinja2 import Environment
from typing_extensions import Self

from ch_tools.common import logging
from ch_tools.common.utils import version_ge

from ..config import get_clickhouse_config
from ..config.clickhouse import ClickhousePort
from .error import ClickhouseError
from .retry import retry
from .utils import _format_str_imatch, _format_str_match

PORTS_PRIORITY = [
    ClickhousePort.HTTPS,
    ClickhousePort.HTTP,
    ClickhousePort.TCP_SECURE,
    ClickhousePort.TCP,
]


class ClickhouseClient:
    """
    ClickHouse client wrapper.
    """

    def __init__(
        self: Self,
        *,
        host: str,
        insecure: bool = False,
        user: Optional[str] = None,
        password: Optional[str] = None,
        ports: Dict[ClickhousePort, int],
        cert_path: Optional[str] = None,
        timeout: int,
        settings: Optional[Dict[str, Any]] = None,
    ):
        self.host = host
        self.insecure = insecure
        self.user = user
        self.ports = ports
        self.cert_path = cert_path
        self.password = password
        self._settings = settings or {}
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

    def _execute_http(
        self,
        query,
        format_,
        post_data,
        timeout,
        stream,
        per_query_settings,
        port,
    ):
        schema = "https" if port == ClickhousePort.HTTPS else "http"
        url = f"{schema}://{self.host}:{self.ports[port]}"
        headers = {}
        if self.user:
            headers["X-ClickHouse-User"] = self.user
        if self.password:
            headers["X-ClickHouse-Key"] = self.password
        verify = self.cert_path if port == ClickhousePort.HTTPS else None
        try:
            if query:
                response = requests.post(
                    url,
                    params={
                        **self._settings,
                        "query": query,
                        **per_query_settings,  # overwrites previous settings
                    },
                    headers=headers,
                    json=post_data,
                    timeout=timeout,
                    stream=stream,
                    verify=verify,
                )
            else:
                # Used for ping
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=timeout,
                    verify=verify,
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

    def _execute_tcp(self, query, format_, port):
        # Private method, we are sure that port is tcps or tcp and presents in config
        cmd = [
            "clickhouse-client",
            "--host",
            self.host,
            "--port",
            str(self.ports[port]),
        ]
        if self.user is not None:
            cmd.extend(("--user", self.user))
        if port == ClickhousePort.TCP_SECURE:
            cmd.append("--secure")
        masked_cmd = cmd.copy()
        if self.password is not None:
            cmd.extend(("--password", self.password))
            masked_cmd.extend(("--password", "*****"))

        if not query:
            raise RuntimeError(1, "Can't send empty query in tcp(s) port")

        # pylint: disable=consider-using-with
        proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE  # type: ignore[arg-type]
        )
        stdout, stderr = proc.communicate(input=query.encode())

        if proc.returncode:
            raise RuntimeError(
                '"{0}" failed with: {1}'.format(masked_cmd, stderr.decode())
            )

        response = stdout.decode().strip()

        if format_ in ("JSON", "JSONCompact"):
            return json.loads(response)

        return response.strip()

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
        port: Optional[ClickhousePort] = None,
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

        if timeout is None:
            timeout = self._timeout

        per_query_settings = settings or {}
        per_query_settings.setdefault("receive_timeout", timeout)

        if port is None:
            for i_port in PORTS_PRIORITY:
                if self.check_port(i_port):
                    port = i_port
                    break

            if port is None:
                raise UserWarning(2, "Can't find any port in clickhouse-server config")

        logging.debug("Executing query: {}", query)
        if port in [ClickhousePort.HTTPS, ClickhousePort.HTTP]:
            return self._execute_http(
                query,
                format_,
                post_data,
                timeout,
                stream,
                per_query_settings,
                port,
            )
        return self._execute_tcp(query, format_, port)

    def query_json_data(
        self: Self,
        query: str,
        query_args: Optional[Dict[str, Any]] = None,
        compact: bool = True,
        post_data: Any = None,
        timeout: Optional[int] = None,
        echo: bool = False,
        dry_run: bool = False,
        stream: bool = False,
        settings: Optional[dict] = None,
        port: Optional[ClickhousePort] = None,
    ) -> Any:
        """
        Execute ClickHouse query formatted as JSON and return data.
        """
        format_ = "JSON"
        if compact:
            format_ = "JSONCompact"

        return self.query(
            query=query,
            query_args=query_args,
            post_data=post_data,
            timeout=timeout,
            format_=format_,
            echo=echo,
            dry_run=dry_run,
            stream=stream,
            settings=settings,
            port=port,
        )["data"]

    def render_query(self, query, **kwargs):
        env = Environment()

        env.globals["version_ge"] = lambda version: version_ge(
            self.get_clickhouse_version(), version
        )
        env.globals["format_str_match"] = _format_str_match
        env.globals["format_str_imatch"] = _format_str_imatch

        template = env.from_string(query)
        return template.render(kwargs)

    def check_port(self, port: ClickhousePort) -> bool:
        return port in self.ports

    def get_port(self, port: ClickhousePort) -> int:
        return self.ports[port]

    def ping(self, port: ClickhousePort) -> str:
        return self.query(query=None, port=port)


def clickhouse_client(ctx: Context) -> ClickhouseClient:
    """
    Return ClickHouse client from the context if it exists.
    Init ClickHouse client and store to the context if it doesn't exist.
    """
    if not ctx.obj.get("chcli"):
        ch_server_config = get_clickhouse_config(ctx)
        tools_config = ctx.obj["config"]["clickhouse"]
        user, password = clickhouse_credentials(ctx)
        ctx.obj["chcli"] = ClickhouseClient(
            host=tools_config["host"],
            ports=ch_server_config.ports,
            user=user,
            password=password,
            cert_path=ch_server_config.cert_path,
            insecure=tools_config["insecure"],
            timeout=tools_config["timeout"],
            settings=tools_config["settings"],
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
