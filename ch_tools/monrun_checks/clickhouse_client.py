import json
import socket
import subprocess
import xml.etree.ElementTree as xml
from enum import Enum
from typing import Dict

import requests

from ch_tools.monrun_checks.exceptions import die


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
    def get(cls, string):
        return cls._map[string] if string in cls._map else ClickhousePort.AUTO

    @classmethod
    def list(cls):
        return cls._map.keys()


class ClickhouseClient:
    """
    ClickHouse client.
    """

    port_settings: Dict[str, str] = {}
    cert_path = "/etc/clickhouse-server/ssl/allCAs.pem"

    def __init__(self):
        self.__get_settings()
        self.host = socket.getfqdn()

    def __execute_http(self, query, port=ClickhousePort.HTTPS):
        # Private method, we are sure that port is https or http and presents in config
        schema = "https" if port == ClickhousePort.HTTPS else "http"
        response = requests.get(
            f"{schema}://{self.host}:{self.port_settings[port]}",
            params={
                "query": query,
            }
            if query
            else {},
            headers={
                "X-ClickHouse-User": "_monitor",
            },
            timeout=10,
            verify=self.cert_path if port == ClickhousePort.HTTPS else None,
        )
        response.raise_for_status()
        if query:
            return response.json()["data"]
        return response.text.strip()  # ping without query

    def __execute_tcp(self, query, port=ClickhousePort.TCP_SECURE):
        # Private method, we are sure that port is tcps or tcp and presents in config
        cmd = [
            "clickhouse-client",
            "--host",
            self.host,
            "--port",
            self.port_settings[port],
            "--user",
            "_monitor",
        ]
        if port == ClickhousePort.TCP_SECURE:
            cmd.append("--secure")

        if not query:
            die(1, "Can't send empty query in tcp(s) port")

        # pylint: disable=consider-using-with
        proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = proc.communicate(input=query.encode())

        if proc.returncode:
            raise RuntimeError('"{0}" failed with: {1}'.format(cmd, stderr.decode()))

        resp = stdout.decode().strip()
        return json.loads(resp)["data"]

    def execute(self, query, compact=True, port=ClickhousePort.AUTO):
        # pylint: disable=redefined-argument-from-local
        if port == ClickhousePort.AUTO:
            for port in ClickhousePort:
                if self.check_port(port):
                    break
            if port == ClickhousePort.AUTO:
                die(2, "Can't find any port in clickhouse-server config")
        if query:
            format_ = "JSON"
            if compact:
                format_ = "JSONCompact"
            query = f"{query} FORMAT {format_}"
        if port in [ClickhousePort.HTTPS, ClickhousePort.HTTP]:
            return self.__execute_http(query, port)
        return self.__execute_tcp(query, port)

    def ping(self, port=ClickhousePort.HTTPS):
        return self.execute(None, port=port)

    def check_port(self, port=ClickhousePort.AUTO):
        if port == ClickhousePort.AUTO:
            return bool(self.port_settings)  # Has any port
        return port in self.port_settings

    def get_port(self, port):
        if port in self.port_settings:
            return self.port_settings[port]
        return 0

    def __get_settings(self):
        result: Dict[str, str] = {}
        try:
            root = xml.parse("/var/lib/clickhouse/preprocessed_configs/config.xml")
            for setting in ClickhousePortHelper.list():
                node = root.find(setting)
                if node is not None:
                    result[ClickhousePortHelper.get(setting)] = str(node.text)
            self.port_settings = result
            if not result:
                die(2, "Can't find any port in clickhouse-server config")
            node = root.find("./openSSL/server/caConfig")
            if node is not None:
                self.cert_path = str(node.text)

        except FileNotFoundError as e:
            die(2, f"clickhouse-server config not found: {e.filename}")

        except Exception as e:
            die(2, f"Failed to parse clickhouse-server config: {e}")

        return result
