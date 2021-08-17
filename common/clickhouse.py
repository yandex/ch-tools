import logging
import socket
from typing import MutableMapping

import requests
from copy import deepcopy

import tenacity
import xmltodict


def retry(exception_types, max_attempts=5, max_interval=5):
    """
    Function decorator that retries wrapped function on failures.
    """
    return tenacity.retry(
        retry=tenacity.retry_if_exception_type(exception_types),
        wait=tenacity.wait_random_exponential(multiplier=0.5, max=max_interval),
        stop=tenacity.stop_after_attempt(max_attempts),
        reraise=True)


class ClickhouseClient:
    """
    ClickHouse client.
    """

    def __init__(self, *, host=socket.getfqdn(), port=8443, user='mdb_admin', insecure=False):
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

    def __init__(self, config):
        self._config = config

    @property
    def macros(self):
        """
        ClickHouse macros.
        """
        macros = self._config['yandex'].get('macros', {})
        return {key: value for key, value in macros.items() if not key.startswith('@')}

    @property
    def cluster_name(self):
        return self.macros['cluster']

    @property
    def zookeeper(self):
        """
        ZooKeeper configuration.
        """
        return self._config['yandex'].get('zookeeper', {})

    def has_disk(self, name):
        storage_configuration = self._config['yandex'].get('storage_configuration', {})
        return name in storage_configuration.get('disks', {})

    def dump(self, mask_secrets=True):
        config = deepcopy(self._config)
        if mask_secrets:
            _mask_secrets(config)

        return xmltodict.unparse(config, pretty=True)

    @staticmethod
    def load():
        return ClickhouseConfig(_load_config('/var/lib/clickhouse/preprocessed_configs/config.xml'))


def _load_config(config_path):
    with open(config_path, 'r') as file:
        return xmltodict.parse(file.read())


def _mask_secrets(config):
    if isinstance(config, MutableMapping):
        for key, value in list(config.items()):
            if isinstance(value, MutableMapping):
                _mask_secrets(config[key])
            elif key in ('password', 'secret_access_key', 'header'):
                config[key] = '*****'
