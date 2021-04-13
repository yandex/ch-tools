import logging
import socket

import requests
import tenacity
import xmltodict


def retry(exception_types, max_attempts=5, max_interval=5):
    """
    Function decorator that retries wrapped function on failures.
    """
    return tenacity.retry(
        retry=tenacity.retry_if_exception_type(exception_types),
        wait=tenacity.wait_random_exponential(multiplier=0.5,
                                              max=max_interval),
        stop=tenacity.stop_after_attempt(max_attempts),
        reraise=True)


class ClickhouseClient:
    """
    ClickHouse client.
    """

    def __init__(self, *, host=socket.getfqdn(), port=8443, user=None, insecure=False):
        self._session = requests.Session()
        self._session.verify = False if insecure else '/etc/clickhouse-server/ssl/allCAs.pem'
        if user:
            self._session.headers['X-ClickHouse-User'] = user
        self._url = f'https://{host}:{port}'
        self._timeout = 60

    @retry(requests.exceptions.ConnectionError)
    def query(self, query, format=None, post_data=None):
        """
        Execute query.
        """
        if format:
            query += f' FORMAT {format}'

        logging.debug('Executing query: %s', query)
        response = self._session.post(self._url,
                                      params={
                                          'query': query,
                                      },
                                      json=post_data,
                                      timeout=self._timeout)

        response.raise_for_status()

        if format in ('JSON', 'JSONCompact'):
            return response.json()

        return response.text.strip()


class ClickhouseConfig:

    def __init__(self, server_config, users_config):
        self._server_config = server_config
        self._users_config = users_config

    def has_disk(self, name):
        storage_configuration = self._server_config['yandex'].get('storage_configuration', {})
        return name in storage_configuration.get('disks', {})

    @staticmethod
    def load():
        return ClickhouseConfig(
            _load_config('/var/lib/clickhouse/preprocessed_configs/config.xml'),
            _load_config('/var/lib/clickhouse/preprocessed_configs/users.xml'))


def _load_config(config_path):
    with open(config_path, 'r') as file:
        return xmltodict.parse(file.read())
