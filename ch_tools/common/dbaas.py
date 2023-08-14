import json


class DbaasConfig:
    def __init__(self, config):
        self._config = config

    @property
    def vtype(self):
        return self._config["vtype"]

    @property
    def cloud_id(self):
        return self._config["cloud"]["cloud_ext_id"]

    @property
    def folder_id(self):
        return self._config["folder"]["folder_ext_id"]

    @property
    def cluster_id(self):
        return self._config["cluster_id"]

    @property
    def cluster_name(self):
        return self._config["cluster_name"]

    @property
    def created_at(self):
        return self._config["created_at"]

    @property
    def shard_count(self):
        subcluster = self._clickhouse_subcluster()
        return len(subcluster["shards"])

    @property
    def host_count(self):
        return len(self._config["cluster_hosts"])

    @property
    def clickhouse_host_count(self):
        subcluster = self._clickhouse_subcluster()
        count = 0
        for shard in subcluster["shards"].values():
            count += len(shard["hosts"])
        return count

    @property
    def shard_hosts(self):
        return self._config["shard_hosts"]

    @property
    def replicas(self):
        return [host for host in self.shard_hosts if host != self.fqdn]

    @property
    def fqdn(self):
        return self._config["fqdn"]

    @property
    def disk_type(self):
        return self._config["disk_type_id"]

    @property
    def disk_size(self):
        return self._config["space_limit"]

    @property
    def flavor(self):
        return self._config["flavor"]["name"]

    @property
    def cpu_fraction(self):
        return self._config["flavor"]["cpu_fraction"]

    @property
    def cpu_limit(self):
        return self._config["flavor"]["cpu_limit"]

    @property
    def cpu_guarantee(self):
        return self._config["flavor"]["cpu_guarantee"]

    @property
    def memory_limit(self):
        return self._config["flavor"]["memory_limit"]

    @property
    def memory_guarantee(self):
        return self._config["flavor"]["memory_guarantee"]

    def _clickhouse_subcluster(self):
        for subcluster in self._config["cluster"]["subclusters"].values():
            if "clickhouse_cluster" in subcluster["roles"]:
                return subcluster

    @staticmethod
    def load():
        with open("/etc/dbaas.conf", "r", encoding="utf-8") as file:
            return DbaasConfig(json.load(file))
