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
    And we execute query on clickhouse01
    """
    SELECT count() FROM system.parts WHERE table='test' and active=1
    """
    Then we get response
    """
    0
    """
