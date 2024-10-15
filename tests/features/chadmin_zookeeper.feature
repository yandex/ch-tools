Feature: chadmin zookeeper commands.

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02


  Scenario: Cleanup all hosts
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS test ON CLUSTER 'cluster'; 
    CREATE DATABASE test ON CLUSTER 'cluster';

    CREATE TABLE test.table_01 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n;

    CREATE TABLE test.table_02 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_02', '{replica}') PARTITION BY n ORDER BY n;

    DETACH TABLE test.table_01 ON CLUSTER 'cluster';
    DETACH TABLE test.table_02 ON CLUSTER 'cluster';
    """

    And we do hosts cleanup on clickhouse01 with fqdn clickhouse01.ch_tools_test,clickhouse02.ch_tools_test and zk root /
    Then the list of children on clickhouse01 for zk node /tables/ are empty

  Scenario: Cleanup all hosts dry run
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS test ON CLUSTER 'cluster'; 
    CREATE DATABASE test ON CLUSTER 'cluster';

    CREATE TABLE test.table_01 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n;

    CREATE TABLE test.table_02 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_02', '{replica}') PARTITION BY n ORDER BY n;

    DETACH TABLE test.table_01 ON CLUSTER 'cluster';
    DETACH TABLE test.table_02 ON CLUSTER 'cluster';
    """

    And we do hosts dry cleanup on clickhouse01 with fqdn clickhouse01.ch_tools_test,clickhouse02.ch_tools_test and zk root /
    Then the list of children on clickhouse01 for zk node /tables/ are equal to
    """
    /tables/table_01
    /tables/table_02
    """

  Scenario: Cleanup single host 
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS test ON CLUSTER 'cluster'; 
    CREATE DATABASE test ON CLUSTER 'cluster';

    CREATE TABLE test.table_01 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n;

    CREATE TABLE test.table_02 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_02', '{replica}') PARTITION BY n ORDER BY n;

    DETACH TABLE test.table_01 ON CLUSTER 'cluster';
    DETACH TABLE test.table_02 ON CLUSTER 'cluster';
    """

    And we do hosts cleanup on clickhouse01 with fqdn clickhouse01.ch_tools_test and zk root /
    Then the list of children on clickhouse01 for zk node /tables/table_01/replicas are equal to
    """
    /tables/table_01/replicas/clickhouse02.ch_tools_test
    """
    And the list of children on clickhouse01 for zk node /tables/table_02/replicas are equal to
    """
    /tables/table_02/replicas/clickhouse02.ch_tools_test
    """

  Scenario: Cleanup single host on non empty node.
    When we execute queries on clickhouse02
    """
    DROP DATABASE IF EXISTS test ON CLUSTER 'cluster'; 
    CREATE DATABASE test ON CLUSTER 'cluster';

    CREATE TABLE test.table_01 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n;

    CREATE TABLE test.table_02 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_02', '{replica}') PARTITION BY n ORDER BY n;

    DETACH TABLE test.table_01;
    DETACH TABLE test.table_02;
    """

    And we do hosts cleanup on clickhouse01 with fqdn clickhouse02.ch_tools_test and zk root /
    Then the list of children on clickhouse02 for zk node /tables/table_01/replicas are equal to
    """
    /tables/table_01/replicas/clickhouse01.ch_tools_test
    """
    And the list of children on clickhouse02 for zk node /tables/table_02/replicas are equal to
    """
    /tables/table_02/replicas/clickhouse01.ch_tools_test
    """

  @require_version_23.1
  Scenario: Remove single host from Replicated database.
    When we execute queries on clickhouse01
    """
    CREATE DATABASE testdb ON CLUSTER 'cluster' ENGINE  = Replicated('/clickhouse/databases/test', 'shard1', '{replica}');
    DETACH DATABASE testdb ON CLUSTER 'cluster';
    """
    And we do hosts cleanup on clickhouse01 with fqdn clickhouse01.ch_tools_test and zk root /
    Then the list of children on clickhouse01 for zk node /clickhouse/databases/test/replicas/ are equal to
    """
    /clickhouse/databases/test/replicas/shard1|clickhouse02.ch_tools_test
    """

  @require_version_23.1
  Scenario: Remove all host from Replicated database.
    When we execute queries on clickhouse01
    """
    CREATE DATABASE testdb ON CLUSTER 'cluster' ENGINE = Replicated('/clickhouse/databases/test', 'shard1', '{replica}');
    DETACH DATABASE testdb ON CLUSTER 'cluster';
    """
    And we do hosts cleanup on clickhouse01 with fqdn clickhouse01.ch_tools_test,clickhouse02.ch_tools_test and zk root /
    Then the list of children on clickhouse01 for zk node /clickhouse/databases/ are empty

  Scenario: Zookeeper recursive delete command
    When we execute chadmin create zk nodes on clickhouse01
    """
      /test/a/b/c
      /test/a/b/d
      /test/a/f
    """
    And we delete zookeepers nodes /test/a on clickhouse01
    Then the list of children on clickhouse01 for zk node /test are empty

  Scenario: Zookeeper delete parent and child nodes
    When we execute chadmin create zk nodes on clickhouse01
    """
      /test/a/b
      /test/c/d
    """
    # Make sure that it is okey to delete the node and its child.
    And we delete zookeepers nodes /test/a,/test/a/b on clickhouse01
    And we delete zookeepers nodes /test/c/d,/test/c on clickhouse01
    Then the list of children on clickhouse01 for zk node /test are empty


  Scenario: Set finished to ddl with removed host.
    # Create the fake cluster with removed host.
    When we put the  clickhouse config to path /etc/clickhouse-server/config.d/cluster.xml with restarting on clickhouse02
    """
    <clickhouse>
        <remote_servers>
          <cluster_with_removed_host>
              <shard>
                  <internal_replication>true</internal_replication>
                  <replica>
                      <host>clickhouse02</host>
                      <port>9000</port>
                  </replica>

                  <replica>
                      <host>zone-host.db.asd.net</host>
                      <port>9000</port>
                  </replica>
              </shard>
          </cluster_with_removed_host>
      </remote_servers>
    </clickhouse>
    """
    And we execute query on clickhouse02
    """
      CREATE DATABASE test_db ON CLUSTER 'cluster_with_removed_host'
    """
    # Make sure that ddl have executed on clickhouse02
    And we sleep for 5 seconds
    And we execute query on clickhouse02
    """
      CREATE TABLE test_db.test ON CLUSTER 'cluster_with_removed_host'  (a int) ENGINE=MergeTree() ORDER BY a
    """
  
    And we do hosts cleanup on clickhouse02 with fqdn zone-host.db.asd.net
    Then there are no unfinished dll queries on clickhouse02
    # It is not necessary, just to do not increase sleep time in nexts steps.
    # We are doing it to reload DDL queue in clickhouse-server
    When we execute command on clickhouse01
    """
    supervisorctl restart clickhouse-server
    """
    When we sleep for 10 seconds
    And we execute command on clickhouse01
    """
    ch-monitoring log-errors -n 5
    """
    Then we get response
    """
    0;OK, 0 errors for last 5 seconds
    """
