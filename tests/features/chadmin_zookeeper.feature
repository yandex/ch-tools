Feature: chadmin zookeeper commands.

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01


  Scenario: Cleanup all hosts
    When we execute chadmin create zk nodes on zookeeper01
    """
    /test/write_sli_part/shard1/replicas/host1.net
    /test/write_sli_part/shard1/replicas/host2.net
    /test/read_sli_part/shard1/replicas/host1.net
    /test/read_sli_part/shard1/replicas/host2.net
    /test/write_sli_part/shard1/log
    """
    And we do hosts cleanup on zookeeper01 with fqdn host1.net,host2.net and zk root /test

    Then the list of children on zookeeper01 for zk node /test/write_sli_part are empty
    And the list of children on zookeeper01 for zk node /test/read_sli_part are empty

  Scenario: Cleanup single host 
    When we execute chadmin create zk nodes on zookeeper01
    """
    /test/write_sli_part/shard1/replicas/host1.net
    /test/write_sli_part/shard1/replicas/host2.net
    /test/read_sli_part/shard1/replicas/host1.net
    /test/read_sli_part/shard1/replicas/host2.net
    /test/write_sli_part/shard1/log
    """
    And we do hosts cleanup on zookeeper01 with fqdn host1.net and zk root /test
    

    Then the list of children on zookeeper01 for zk node /test/write_sli_part/shard1/replicas/ are equal to
    """
    /test/write_sli_part/shard1/replicas/host2.net
    """
    And the list of children on zookeeper01 for zk node  /test/read_sli_part/shard1/replicas/ are equal to
    """
    /test/read_sli_part/shard1/replicas/host2.net
    """


  Scenario: Remove single host from Replicated database.
    # Imitate that we got nodes in zk from replicated database.
    When we execute chadmin create zk nodes on zookeeper01
    """
    '/test/clickhouse/databases/test/shard1/replicas/shard1|host1.net'
    '/test/clickhouse/databases/test/shard1/replicas/shard1|host2.net'
    '/test/clickhouse/databases/test/shard1/counter'
    """
    And we do hosts cleanup on zookeeper01 with fqdn host1.net and zk root /test
    Then the list of children on zookeeper01 for zk node /test/clickhouse/databases/test/shard1/replicas/ are equal to
    """
    /test/clickhouse/databases/test/shard1/replicas/shard1|host2.net
    """

  Scenario: Remove all host from Replicated database.
    # Imitate that we got nodes in zk from replicated database.
    When we execute chadmin create zk nodes on zookeeper01
    """
    '/test/clickhouse/databases/test/shard1/replicas/shard1|host1.net'
    '/test/clickhouse/databases/test/shard1/replicas/shard1|host2.net'
    '/test/clickhouse/databases/test/shard1/counter'
    """
    And we do hosts cleanup on zookeeper01 with fqdn host1.net,host2.net and zk root /test
    Then the list of children on zookeeper01 for zk node test/clickhouse/databases/test/ are empty

  Scenario: Zookeeper recursive delete command
    When we execute chadmin create zk nodes on zookeeper01
    """
      /test/a/b/c
      /test/a/b/d
      /test/a/f
    """
    And we delete zookeepers nodes /test/a on zookeeper01
    Then the list of children on zookeeper01 for zk node /test are empty

  Scenario: Zookeeper delete parent and child nodes
    When we execute chadmin create zk nodes on zookeeper01
    """
      /test/a/b
      /test/c/d
    """
    # Make sure that it is okey to delete the node and its child.
    And we delete zookeepers nodes /test/a,/test/a/b on zookeeper01
    And we delete zookeepers nodes /test/c/d,/test/c on zookeeper01
    Then the list of children on zookeeper01 for zk node /test are empty


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
    And we execute query on clickhouse02
    """
      CREATE TABLE test_db.test ON CLUSTER 'cluster_with_removed_host'  (a int) ENGINE=MergeTree() ORDER BY a
    """
    
    And we do hosts cleanup on clickhouse02 with fqdn zone-host.db.asd.net
    Then there are no unfinished dll queries on clickhouse02
