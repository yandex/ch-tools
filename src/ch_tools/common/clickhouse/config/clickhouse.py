import os.path

from ch_tools.common.utils import deep_merge

from .path import (
    CLICKHOUSE_SERVER_CLUSTER_CONFIG_PATH,
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
        return self._config.get("clickhouse", self._config.get("yandex", {}))

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

    def has_disk(self, name):
        storage_configuration = self._config_root.get("storage_configuration", {})
        return name in storage_configuration.get("disks", {})

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
        deep_merge(config, _load_config(CLICKHOUSE_SERVER_CLUSTER_CONFIG_PATH))
        for file in os.listdir(CLICKHOUSE_SERVER_CONFIGD_PATH):
            deep_merge(
                config, _load_config(os.path.join(CLICKHOUSE_SERVER_CONFIGD_PATH, file))
            )

        # Process includes
        root_key = next(iter(config))
        root_section = config[root_key]
        for key, config_section in root_section.copy().items():
            if not isinstance(config_section, dict):
                continue

            include = config_section.get("@incl")
            if not include:
                continue

            if include != key and include in root_section:
                root_section[key] = root_section[include]
                del root_section[include]

            del config_section["@incl"]

        return ClickhouseConfig(config, preprocessed=False)
