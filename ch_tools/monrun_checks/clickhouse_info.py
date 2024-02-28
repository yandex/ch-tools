import functools

from ch_tools.common.clickhouse.client.clickhouse_client import clickhouse_client


class ClickhouseInfo:
    @classmethod
    @functools.lru_cache(maxsize=1)
    def get_replicas(cls, ctx):
        """
        Get hostnames of replicas.
        """
        cluster = cls.get_cluster(ctx)
        query = f"""
            SELECT host_name
            FROM system.clusters
            WHERE cluster = '{cluster}'
            AND shard_num = (SELECT shard_num FROM system.clusters
                            WHERE host_name = hostName() AND cluster = '{cluster}')
            """
        return [row[0] for row in clickhouse_client(ctx).query_json_data(query=query)]

    @classmethod
    @functools.lru_cache(maxsize=1)
    def get_cluster(cls, ctx):
        """
        Get cluster identifier.
        """
        query = "SELECT substitution FROM system.macros WHERE macro = 'cluster'"
        return clickhouse_client(ctx).query_json_data(query=query)[0][0]
