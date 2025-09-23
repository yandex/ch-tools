import os.path
from typing import Any

from .path import CLICKHOUSE_USERS_XML_CONFIG_PATH, CLICKHOUSE_USERS_YAML_CONFIG_PATH
from .utils import dump_config, load_config


class ClickhouseUsersConfig:
    """
    ClickHouse users config (users.xml).
    """

    def __init__(self, config: Any) -> None:
        self._config = config

    def dump(self, mask_secrets: bool = True) -> Any:
        return dump_config(self._config, mask_secrets=mask_secrets)

    def dump_xml(self, mask_secrets: bool = True) -> Any:
        return dump_config(self._config, mask_secrets=mask_secrets, xml_format=True)

    @staticmethod
    def load() -> "ClickhouseUsersConfig":
        config_path = None
        for path in (
            CLICKHOUSE_USERS_XML_CONFIG_PATH,
            CLICKHOUSE_USERS_YAML_CONFIG_PATH,
        ):
            if os.path.exists(path):
                config_path = path
                break

        if not config_path:
            raise RuntimeError("Users configuration file not found")

        return ClickhouseUsersConfig(load_config(config_path, "users.d"))
