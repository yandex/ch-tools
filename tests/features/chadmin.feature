Feature: chadmin commands.

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02


  Scenario: Check wait replication sync
    When we execute command on clickhouse01
    """
    chadmin wait replication-sync -t 10 -p 1
    """
