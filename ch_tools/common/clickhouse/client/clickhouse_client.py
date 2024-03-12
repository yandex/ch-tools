import json
import logging
import subprocess
import xml.etree.ElementTree as xml
from datetime import timedelta
from enum import Enum
from typing import Any, Dict, Optional

import requests
from jinja2 import Environment
from typing_extensions import Self

from ch_tools.common.clickhouse.config.path import (
    CLICKHOUSE_CERT_CONFIG_PATH,
    CLICKHOUSE_CERT_PATH_DEFAULT,
    CLICKHOUSE_SERVER_PREPROCESSED_CONFIG_PATH,
)
from ch_tools.common.utils import version_ge

from .error import ClickhouseError
from .retry import retry
from .utils import _format_str_imatch, _format_str_match


class ClickhousePort(Enum):
    HTTPS = 4
    HTTP = 3
    TCP_SECURE = 2
    TCP = 1
    AUTO = 0  # Select any available port


class ClickhousePortHelper:
    _map = {
        "https_port": ClickhousePort.HTTPS,
        "http_port": ClickhousePort.HTTP,
        "tcp_port_secure": ClickhousePort.TCP_SECURE,
        "tcp_port": ClickhousePort.TCP,
    }

    @classmethod
    def get(
        cls, port: str, default: ClickhousePort = ClickhousePort.AUTO
    ) -> ClickhousePort:
        return cls._map.get(port, default)

    @classmethod
    def list(cls):
        return cls._map.keys()


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
        ports: Dict[ClickhousePort, str],
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
            self.ports[port],
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
        port: ClickhousePort = ClickhousePort.AUTO,
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

        found_port = port
        if found_port == ClickhousePort.AUTO:
            for port_ in ClickhousePort:
                if self.check_port(port_):
                    found_port = port_
                    break
            if found_port == ClickhousePort.AUTO:
                raise UserWarning(2, "Can't find any port in clickhouse-server config")

        logging.debug("Executing query: %s", query)
        if found_port in [ClickhousePort.HTTPS, ClickhousePort.HTTP]:
            return self._execute_http(
                query,
                format_,
                post_data,
                timeout,
                stream,
                per_query_settings,
                found_port,
            )
        return self._execute_tcp(query, format_, found_port)

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
        port: ClickhousePort = ClickhousePort.AUTO,
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

    def check_port(self, port=ClickhousePort.AUTO):
        if port == ClickhousePort.AUTO:
            return bool(self.ports)  # Has any port
        return port in self.ports

    def get_port(self, port):
        return self.ports.get(port, 0)

    def ping(self, port=ClickhousePort.AUTO):
        return self.query(query=None, port=port)


def clickhouse_client(ctx):
    """
    Return ClickHouse client from the context if it exists.
    Init ClickHouse client and store to the context if it doesn't exist.
    """
    if not ctx.obj.get("chcli"):
        ports, cert_path = get_ports()
        config = ctx.obj["config"]["clickhouse"]
        user, password = clickhouse_credentials(ctx)
        ctx.obj["chcli"] = ClickhouseClient(
            host=config["host"],
            insecure=config["insecure"],
            user=user,
            password=password,
            ports=ports,
            cert_path=cert_path,
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


def get_ports():
    ports: Dict[ClickhousePort, str] = {}
    try:
        root = xml.parse(CLICKHOUSE_SERVER_PREPROCESSED_CONFIG_PATH)
        for setting in ClickhousePortHelper.list():
            node = root.find(setting)
            if node is not None:
                ports[ClickhousePortHelper.get(setting)] = str(node.text)
        if not ports:
            raise UserWarning(2, "Can't find any port in clickhouse-server config")
        node = root.find(CLICKHOUSE_CERT_CONFIG_PATH)
        cert_path = CLICKHOUSE_CERT_PATH_DEFAULT
        if node is not None:
            cert_path = str(node.text)

    except FileNotFoundError as e:
        raise UserWarning(2, f"clickhouse-server config not found: {e.filename}")

    except Exception as e:
        raise UserWarning(2, f"Failed to parse clickhouse-server config: {e}")

    return ports, cert_path
