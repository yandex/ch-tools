from typing import Any

from .path import CLICKHOUSE_USERS_CONFIG_PATH
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
        return ClickhouseUsersConfig(
            load_config(CLICKHOUSE_USERS_CONFIG_PATH, "users.d")
        )
