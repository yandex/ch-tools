import os
from typing import Any, Optional, Tuple

from ...utils import first_value
from .path import (
    CLICKHOUSE_KEEPER_CONFIG_PATH,
    CLICKHOUSE_SERVER_PREPROCESSED_CONFIG_PATH,
)
from .utils import dump_config, load_config


class ClickhouseKeeperConfig:
    """
    ClickHouse keeper server config (config.xml).
    """

    def __init__(self, config: Any, config_path: str) -> None:
        self._config = config
        self._config_path = config_path

    @property
    def _clickhouse(self) -> Any:
        return first_value(self._config)

    @property
    def _keeper_server(self) -> Any:
        return self._clickhouse.get("keeper_server", {})

    @property
    def port_pair(self) -> Tuple[int, bool]:
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
    def tls_cert_path(self) -> Optional[str]:
        return (
            self._clickhouse.get("openSSL", {})
            .get("server", {})
            .get("certificateFile", None)
        )

    @property
    def snapshots_dir(self) -> Optional[str]:
        return self._keeper_server.get("snapshot_storage_path")

    @property
    def storage_dir(self) -> Optional[str]:
        return self._keeper_server.get("storage_path")

    @property
    def separated(self) -> bool:
        """
        Return True if ClickHouse Keeper is configured to run in separate process.
        """
        return self._config_path == CLICKHOUSE_KEEPER_CONFIG_PATH

    def dump(self, mask_secrets: bool = True) -> str:
        return dump_config(self._config, mask_secrets=mask_secrets)

    def dump_xml(self, mask_secrets: bool = True) -> str:
        return dump_config(self._config, mask_secrets=mask_secrets, xml_format=True)

    @staticmethod
    def load() -> "ClickhouseKeeperConfig":
        if os.path.exists(CLICKHOUSE_KEEPER_CONFIG_PATH):
            config_path = CLICKHOUSE_KEEPER_CONFIG_PATH
        else:
            config_path = CLICKHOUSE_SERVER_PREPROCESSED_CONFIG_PATH

        config = load_config(config_path)
        return ClickhouseKeeperConfig(config, config_path)
