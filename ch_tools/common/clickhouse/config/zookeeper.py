from typing import Any, Optional


class ClickhouseZookeeperConfig:
    """
    ZooKeeper section of ClickHouse server config.
    """

    def __init__(self, config: dict) -> None:
        self._config = config

    def is_empty(self) -> bool:
        return not bool(self._config)

    @property
    def nodes(self) -> list:
        value = self._config["node"]
        if isinstance(value, list):
            return value

        return [value]

    @property
    def root(self) -> Optional[Any]:
        return self._config.get("root")

    @property
    def identity(self) -> Optional[Any]:
        return self._config.get("identity")
