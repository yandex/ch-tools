import os.path
from enum import Enum
from typing import Any, Dict

from ch_tools.common.clickhouse.config.storage_configuration import (
    ClickhouseStorageConfiguration,
)

from ...utils import first_value
from .path import (
    CLICKHOUSE_CERT_PATH_DEFAULT,
    CLICKHOUSE_SERVER_CONFIG_PATH,
    CLICKHOUSE_SERVER_PREPROCESSED_CONFIG_PATH,
)
from .utils import dump_config, load_config
from .zookeeper import ClickhouseZookeeperConfig


class ClickhousePort(Enum):
    TCP = 1
    TCP_SECURE = 2
    HTTP = 3
    HTTPS = 4


class ClickhouseConfig:
    """
    ClickHouse server config (config.xml).
    """

    def __init__(self, config: Any, preprocessed: Any) -> None:
        self._config = config
        self.preprocessed = preprocessed

    @property
    def _config_root(self) -> dict:
        return first_value(self._config)

    @property
    def macros(self) -> dict:
        """
        ClickHouse macros.
        """
        macros = self._config_root.get("macros", {})
        return {key: value for key, value in macros.items() if not key.startswith("@")}

    @property
    def cluster_name(self) -> Any:
        return self.macros["cluster"]

    @property
    def zookeeper(self) -> ClickhouseZookeeperConfig:
        """
        ZooKeeper configuration.
        """
        return ClickhouseZookeeperConfig(self._config_root.get("zookeeper", {}))

    @property
    def storage_configuration(self) -> ClickhouseStorageConfiguration:
        return ClickhouseStorageConfiguration(
            self._config_root.get("storage_configuration", {})
        )

    @property
    def ports(self) -> Dict[ClickhousePort, int]:
        settings = {
            "tcp_port": ClickhousePort.TCP,
            "tcp_port_secure": ClickhousePort.TCP_SECURE,
            "http_port": ClickhousePort.HTTP,
            "https_port": ClickhousePort.HTTPS,
        }

        result = {}
        for setting_name, port in settings.items():
            value = self._config_root.get(setting_name)
            if value:
                result[port] = int(value)

        return result

    @property
    def cert_path(self) -> str:
        openssl_server_config = self._config_root.get("openSSL", {}).get("server", {})
        return openssl_server_config.get("caConfig", CLICKHOUSE_CERT_PATH_DEFAULT)

    def dump(self, mask_secrets: bool = True) -> Any:
        return dump_config(self._config, mask_secrets=mask_secrets)

    def dump_xml(self, mask_secrets: bool = True) -> Any:
        return dump_config(self._config, mask_secrets=mask_secrets, xml_format=True)

    @staticmethod
    def load(try_preprocessed: bool = False) -> "ClickhouseConfig":
        if try_preprocessed and os.path.exists(
            CLICKHOUSE_SERVER_PREPROCESSED_CONFIG_PATH
        ):
            config = load_config(CLICKHOUSE_SERVER_PREPROCESSED_CONFIG_PATH)
            return ClickhouseConfig(config, preprocessed=True)

        config = load_config(CLICKHOUSE_SERVER_CONFIG_PATH)
        return ClickhouseConfig(config, preprocessed=False)
