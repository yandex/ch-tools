import functools

from ch_tools.monrun_checks.clickhouse_client import ClickhouseClient, ClickhousePort


class ClickhouseInfo:
    @classmethod
    @functools.lru_cache(maxsize=1)
    def get_versions_count(cls, ctx):
        """
        Count different clickhouse versions in cluster.
        """
        ch_client = ClickhouseClient(ctx)
        #  I believe that all hosts in cluster have the same port set, so check current for security port
        remote_command = (
            "remoteSecure"
            if ch_client.check_port(ClickhousePort.TCP_SECURE)
            else "remote"
        )
        replicas = ",".join(cls.get_replicas(ctx))
        query = f"""SELECT uniq(version()) FROM {remote_command}('{replicas}', system.one)"""
        return int(ch_client.execute(query)[0][0])

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
        return [row[0] for row in ClickhouseClient(ctx).execute(query)]

    @classmethod
    @functools.lru_cache(maxsize=1)
    def get_cluster(cls, ctx):
        """
        Get cluster identifier.
        """
        query = "SELECT substitution FROM system.macros WHERE macro = 'cluster'"
        return ClickhouseClient(ctx).execute(query)[0][0]
