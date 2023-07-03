import os

from .path import (
    CLICKHOUSE_KEEPER_CONFIG_PATH,
    CLICKHOUSE_SERVER_PREPROCESSED_CONFIG_PATH,
)
from .utils import _dump_config, _load_config


class ClickhouseKeeperConfig:
    """
    ClickHouse keeper server config (config.xml).
    """

    def __init__(self, config, config_path):
        self._config = config
        self._config_path = config_path

    @property
    def _clickhouse(self):
        return self._config.get("clickhouse", self._config.get("yandex", {}))

    @property
    def _keeper_server(self):
        return self._clickhouse.get("keeper_server", {})

    @property
    def port_pair(self):
        """
        :returns tuple (ClickHouse port, port is secure)
          If both <tcp_port> and <tcp_port_secure> are present, a secure port
          is returned.
        """
        secure_port = self._keeper_server.get("tcp_port_secure")
        if secure_port is not None:
            return int(secure_port), True

        return int(self._keeper_server.get("tcp_port", 0)), False

    @property
    def snapshots_dir(self):
        return self._keeper_server.get("snapshot_storage_path")

    @property
    def storage_dir(self):
        return self._keeper_server.get("storage_path")

    @property
    def separated(self):
        """
        Return True if ClickHouse Keeper is configured to run in separate process.
        """
        return self._config_path == CLICKHOUSE_KEEPER_CONFIG_PATH

    def dump(self, mask_secrets=True):
        return _dump_config(self._config, mask_secrets=mask_secrets)

    def dump_xml(self, mask_secrets=True):
        return _dump_config(self._config, mask_secrets=mask_secrets, xml_format=True)

    @staticmethod
    def load():
        if os.path.exists(CLICKHOUSE_KEEPER_CONFIG_PATH):
            config_path = CLICKHOUSE_KEEPER_CONFIG_PATH
        else:
            config_path = CLICKHOUSE_SERVER_PREPROCESSED_CONFIG_PATH

        config = _load_config(config_path)
        return ClickhouseKeeperConfig(config, config_path)
