import requests
import socket
import xml.etree.ElementTree as xml


class ClickhouseClient:
    port_settings = {}
    cert_path = '/etc/clickhouse-server/ssl/allCAs.pem'

    def __init__(self):
        self.__get_settings()
        self.host = socket.gethostname()
        if 'https_port' in self.port_settings:
            self.protocol = 'https'
            self.port = self.port_settings['https_port']
            self.verify = self.cert_path
        else:
            self.protocol = 'http'
            self.port = self.port_settings['http_port']
            self.verify = None

    def execute(self, query, compact=True):
        format = 'JSON'
        if compact:
            format = 'JSONCompact'
        response = requests.get(
            f'{self.protocol}://{self.host}:{self.port}',
            params={
                'query': f'{query} FORMAT {format}',
            },
            headers={
                'X-ClickHouse-User': 'mdb_monitor',
            },
            timeout=10,
            verify=self.verify)
        response.raise_for_status()
        return response.json()['data']

    def __get_settings(self):
        result = {}
        try:
            root = xml.parse('/var/lib/clickhouse/preprocessed_configs/config.xml')
            for setting in ('http_port', 'https_port'):
                node = root.find(setting)
                if node is not None:
                    result[setting] = node.text
            self.port_settings = result
            if not result:
                self.__die(2, 'Can\'t find http or https port in clickhouse-server config')
            node = root.find('./openSSL/server/certificateFile')
            if node is not None:
                self.cert_path = node.text

        except FileNotFoundError as e:
            self.__die(2, f'clickhouse-server config not found: {e.filename}')

        except Exception as e:
            self.__die(2, f'Failed to parse clickhouse-server config: {e}')

        return result

    def __die(self, status, message):
        raise UserWarning(status, message)
