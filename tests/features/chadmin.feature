Feature: chadmin commands.

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test ON CLUSTER 'cluster';

    CREATE TABLE IF NOT EXISTS test.table_01 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n;
    """


  Scenario: Check wait replication sync
    When we execute command on clickhouse01
    """
    chadmin wait replication-sync --total-timeout 10 --replica-timeout 3 -p 1 -w 4
    """
    When we execute query on clickhouse01
    """
    SYSTEM STOP FETCHES
    """
    And we execute query on clickhouse02
    """
    INSERT INTO test.table_01 SELECT number FROM numbers(100)
    """
    And we sleep for 5 seconds
    When we try to execute command on clickhouse01
    """
    chadmin wait replication-sync --total-timeout 10 --replica-timeout 3 -p 1 -w 4
    """
    Then it fails with response contains
    """
    Timeout while runnung SYNC REPLICA on
    """
    When we execute query on clickhouse01
    """
    SYSTEM START FETCHES
    """
    When we execute command on clickhouse01
    """
    chadmin wait replication-sync --total-timeout 10 --replica-timeout 3 -p 1 -w 4
    """
