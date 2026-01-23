Feature: chadmin partitions commands.

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And we have executed queries on clickhouse01
    """
      DROP TABLE IF EXISTS test;
      CREATE TABLE test(a int) ENGINE=MergeTree() ORDER BY a PARTITION BY a;
      INSERT INTO test SELECT 1;
      INSERT INTO test SELECT 2;
      INSERT INTO test SELECT 3;
    """

  Scenario Outline: Detach and attach with precalculated json.
    When we execute command on clickhouse01
    """
    clickhouse client --query "<query>" > /tmp/json
    """
    And we execute command on clickhouse01
    """
    chadmin <test_object> detach <args>
    """
    And we execute query on clickhouse01
    """
    SELECT count() FROM system.parts WHERE table='test' AND active=1
    """
    Then we get response
    """
    0
    """

    When we execute command on clickhouse01
    """
    chadmin <test_object> attach <args>
    """
    And we execute query on clickhouse01
    """
    SELECT count() FROM system.parts WHERE table='test' AND active=1
    """
    Then we get response
    """
    3
    """
    Examples:
        | query                                                                                   | test_object | args                                      |
        | SELECT database, table, partition_id FROM system.parts WHERE table='test' FORMAT JSON   | partition   | --use-partition-list-from-json /tmp/json  |
        | SELECT database, table, name FROM system.parts WHERE table='test' FORMAT JSON           | part        | --use-part-list-from-json /tmp/json       |

  Scenario: Reattach with precalculated json.
    When we execute command on clickhouse01
    """
    clickhouse client --query "SELECT database, table, partition_id FROM system.parts WHERE table='test' FORMAT JSON" > /tmp/json
    """
    And we execute command on clickhouse01
    """
    chadmin partition reattach --use-partition-list-from-json /tmp/json
    """
    And we execute query on clickhouse01
    """
    SELECT count() FROM system.parts WHERE table='test' AND active=1
    """
    Then we get response
    """
    3
    """

  Scenario: Delete with precalculated json.
    When we execute command on clickhouse01
    """
    clickhouse client --query "SELECT database, table, name FROM system.parts WHERE table='test' FORMAT JSON" > /tmp/json
    """
    And we execute command on clickhouse01
    """
    chadmin part detach --use-part-list-from-json /tmp/json
    """
    And we execute command on clickhouse01
    """
    clickhouse client --query "SELECT database, table, name FROM system.detached_parts WHERE table='test' FORMAT JSON" > /tmp/json_detached
    """
    And we execute command on clickhouse01
    """
    chadmin part delete --detached --use-part-list-from-json /tmp/json_detached
    """
    And we execute query on clickhouse01
    """
    SELECT count() FROM system.detached_parts WHERE table='test'
    """
    Then we get response
    """
    0
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM system.parts WHERE table='test' and active=1
    """
    Then we get response
    """
    0
    """

  @require_version_23.1
  Scenario: Drop detached parts.
    When we execute command on clickhouse01
    """
    chadmin part detach -t test
    """
    And we execute command on clickhouse01
    """
    mkdir /var/lib/clickhouse/data/default/test/detached/1_1_1_0_try1
    """
    And we execute command on clickhouse01
    """
    chadmin part delete -t test --detached
    """
    Then we get response contains
    """
    detached/1_1_1_0_try1
    """
    When we execute command on clickhouse01
    """
    clickhouse client --query "SELECT count() FROM system.detached_parts WHERE table='test'"
    """
    Then we get response
    """
    0
    """


  Scenario: Move partitions.
    When we execute queries on clickhouse01
    """
    CREATE DATABASE db;
    CREATE TABLE db.src1 (a UInt32) ENGINE=MergeTree() ORDER by a PARTITION BY a;
    CREATE TABLE db.src2 (a UInt32) ENGINE=MergeTree() ORDER by a;
    CREATE TABLE db.dst1 (a UInt32) ENGINE=MergeTree() ORDER by a PARTITION BY a;
    CREATE TABLE db.dst2 (a UInt32) ENGINE=MergeTree() ORDER by a;


    INSERT INTO db.src1 SELECT number FROM numbers(5);
    INSERT INTO db.src2 SELECT number FROM numbers(5);
    """
    When we execute command on clickhouse01
    """
    chadmin partition move -D db -T dst1 -d db -t src1
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM db.dst1
    """
    Then we get response
    """
    5
    """
    When we execute command on clickhouse01
    """
    chadmin partition move -D db -T dst2 -d db -t src2
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM db.dst2
    """
    Then we get response
    """
    5
    """
