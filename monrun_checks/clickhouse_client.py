import requests
import socket
import subprocess
import json
import xml.etree.ElementTree as xml
from enum import Enum


class ClickhousePort(Enum):
    https = 4
    http = 3
    tcps = 2
    tcp = 1
    auto = 0  # Select any available port


class ClickhousePortHelper:
    _map = {
        'https_port' : ClickhousePort.https,
        'http_port': ClickhousePort.http,
        'tcp_port_secure' : ClickhousePort.tcps,
        'tcp_port' : ClickhousePort.tcp,
    }

    @classmethod
    def get(cls, string):
        return cls._map[string] if string in cls._map else ClickhousePort.auto

    @classmethod
    def list(cls):
        return cls._map.keys()


class ClickhouseClient:
    port_settings = {}
    cert_path = '/etc/clickhouse-server/ssl/allCAs.pem'

    def __init__(self):
        self.__get_settings()
        self.host = socket.gethostname()

    def __execute_http(self, query, port=ClickhousePort.https):
        # Private method, we are sure that port is https or http and presents in config
        schema = 'https' if port == ClickhousePort.https else 'http'
        response = requests.get(
            f'{schema}://{self.host}:{self.port_settings[port]}',
            params={
                'query': query,
            } if query else {},
            headers={
                'X-ClickHouse-User': 'mdb_monitor',
            },
            timeout=10,
            verify=self.cert_path if port == ClickhousePort.https else None)
        response.raise_for_status()
        if query:
            return response.json()['data']
        return response.text.strip()  # ping without query

    def __execute_tcp(self, query, port=ClickhousePort.tcps):
        # Private method, we are sure that port is tcps or tcp and presents in config
        cmd = [
            'clickhouse-client',
            '--host', self.host,
            '--port', self.port_settings[port],
            '--user', 'mdb_monitor',
        ]
        if port == ClickhousePort.tcps:
            cmd.append('--secure')

        if not query:
            self.__die(1, "Can't send empty query in tcp(s) port")

        proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate(input=query.encode())

        if proc.returncode:
            raise RuntimeError('"{0}" failed with: {1}'.format(cmd, stderr.decode()))

        resp = stdout.decode().strip()
        return json.loads(resp)['data']

    def execute(self, query, compact=True, port=ClickhousePort.auto):
        if port == ClickhousePort.auto:
            for port in ClickhousePort:
                if self.check_port(port):
                    break
            if port == ClickhousePort.auto:
                self.__die(2, 'Can\'t find any port in clickhouse-server config')
        if query:
            format = 'JSON'
            if compact:
                format = 'JSONCompact'
            query = f'{query} FORMAT {format}'
        if port in [ClickhousePort.https, ClickhousePort.http]:
            return self.__execute_http(query, port)
        return self.__execute_tcp(query, port)

    def ping(self, port=ClickhousePort.https):
        return self.execute(None, port=port)

    def check_port(self, port=ClickhousePort.auto):
        if port == ClickhousePort.auto:
            return bool(self.port_settings)  # Has any port
        return port in self.port_settings

    def get_port(self, port):
        if port in self.port_settings:
            return self.port_settings[port]
        return 0

    def __get_settings(self):
        result = {}
        try:
            root = xml.parse('/var/lib/clickhouse/preprocessed_configs/config.xml')
            for setting in (ClickhousePortHelper.list()):
                node = root.find(setting)
                if node is not None:
                    result[ClickhousePortHelper.get(setting)] = node.text
            self.port_settings = result
            if not result:
                self.__die(2, 'Can\'t find any port in clickhouse-server config')
            node = root.find('./openSSL/server/caConfig')
            if node is not None:
                self.cert_path = node.text

        except FileNotFoundError as e:
            self.__die(2, f'clickhouse-server config not found: {e.filename}')

        except Exception as e:
            self.__die(2, f'Failed to parse clickhouse-server config: {e}')

        return result

    def __die(self, status, message):
        raise UserWarning(status, message)
