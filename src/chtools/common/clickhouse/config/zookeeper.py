class ClickhouseZookeeperConfig:
    """
    ZooKeeper section of ClickHouse server config.
    """

    def __init__(self, config):
        self._config = config

    @property
    def nodes(self):
        value = self._config["node"]
        if isinstance(value, list):
            return value

        return [value]

    @property
    def root(self):
        return self._config.get("root")

    @property
    def identity(self):
        return self._config.get("identity")
