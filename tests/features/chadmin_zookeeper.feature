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
