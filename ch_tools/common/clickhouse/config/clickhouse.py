import os.path

from ch_tools.common.clickhouse.config.storage_configuration import (
    ClickhouseStorageConfiguration,
)

from ...utils import first_value
from .path import (
    CLICKHOUSE_SERVER_CONFIGD_PATH,
    CLICKHOUSE_SERVER_MAIN_CONFIG_PATH,
    CLICKHOUSE_SERVER_PREPROCESSED_CONFIG_PATH,
)
from .utils import _dump_config, _load_config
from .zookeeper import ClickhouseZookeeperConfig


class ClickhouseConfig:
    """
    ClickHouse server config (config.xml).
    """

    def __init__(self, config, preprocessed):
        self._config = config
        self.preprocessed = preprocessed

    @property
    def _config_root(self) -> dict:
        return first_value(self._config)

    @property
    def macros(self):
        """
        ClickHouse macros.
        """
        macros = self._config_root.get("macros", {})
        return {key: value for key, value in macros.items() if not key.startswith("@")}

    @property
    def cluster_name(self):
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

    def dump(self, mask_secrets=True):
        return _dump_config(self._config, mask_secrets=mask_secrets)

    def dump_xml(self, mask_secrets=True):
        return _dump_config(self._config, mask_secrets=mask_secrets, xml_format=True)

    @staticmethod
    def load(try_preprocessed=True):
        # Load preprocessed server config if exists and try_preprocessed
        if try_preprocessed and os.path.exists(
            CLICKHOUSE_SERVER_PREPROCESSED_CONFIG_PATH
        ):
            return ClickhouseConfig(
                _load_config(CLICKHOUSE_SERVER_PREPROCESSED_CONFIG_PATH),
                preprocessed=True,
            )

        # Otherwise load all server config files and perform manual merge and processing
        config = _load_config(CLICKHOUSE_SERVER_MAIN_CONFIG_PATH)
        if os.path.exists(CLICKHOUSE_SERVER_CONFIGD_PATH):
            for file in os.listdir(CLICKHOUSE_SERVER_CONFIGD_PATH):
                _merge_configs(
                    config,
                    _load_config(os.path.join(CLICKHOUSE_SERVER_CONFIGD_PATH, file)),
                )

        # Process includes
        root_section = first_value(config)
        include_file = root_section.get("include_from")
        if include_file:
            include_config = first_value(_load_config(include_file))
            _apply_config_directives(root_section, include_config)

        return ClickhouseConfig(config, preprocessed=False)


def _merge_configs(main_config, additional_config):
    for key, value in additional_config.items():
        if key not in main_config:
            main_config[key] = value
            continue

        if isinstance(main_config[key], dict) and isinstance(value, dict):
            _merge_configs(main_config[key], value)
            continue

        if value is not None:
            main_config[key] = value


def _apply_config_directives(config_section, include_config):
    for key, item in config_section.items():
        if not isinstance(item, dict):
            continue

        include = item.get("@incl")
        if include:
            config_section[key] = include_config[include]
            continue

        _apply_config_directives(item, include_config)
