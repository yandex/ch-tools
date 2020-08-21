from cloud.mdb.clickhouse.tools.monrun_checks.clickhouse_client import ClickhouseClient


class ClickhouseInfo(object):

    @classmethod
    def get_versions_count(cls):
        """
        Count different clickhouse versions in cluster.
        """
        replicas = cls.get_replicas()
        query = '''
            SELECT uniq(version()) FROM remote('{replicas}', system.one)
            '''.format(
                replicas=','.join(replicas))
        return int(ClickhouseClient().execute(query)[0][0])

    @classmethod
    def get_replicas(cls):
        """
        Get hostnames of replicas and cache result.
        """
        global replicas_cached
        if replicas_cached is None:
            replicas_cached = cls.get_replicas_impl()
        return replicas_cached

    @classmethod
    def get_replicas_impl(cls):
        """
        Get hostnames of replicas.
        """
        cluster = cls.get_cluster()
        query = '''
            SELECT host_name
            FROM system.clusters
            WHERE cluster = '{cluster}'
            AND shard_num = (SELECT shard_num FROM system.clusters
                            WHERE host_name = hostName() AND cluster = '{cluster}')
            '''.format(cluster=cluster)
        return [row[0] for row in ClickhouseClient().execute(query)]

    @classmethod
    def get_cluster(cls):
        """
        Get cluster identifier.
        """
        query = 'SELECT substitution FROM system.macros WHERE macro = \'cluster\''
        return ClickhouseClient().execute(query)[0][0]
