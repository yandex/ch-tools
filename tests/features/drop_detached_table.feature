Feature: chadmin delete detached table commands

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;

    CREATE TABLE IF NOT EXISTS test_db.test_table (n Int32)
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO test_db.test_table (n) SELECT number FROM system.numbers LIMIT 10;
    DETACH TABLE test_db.test_table;
    """

  Scenario: Drop detached table from local disk
    When we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/data/test_db/test_table/ | wc -l
    """
    Then we get response
    """
    3
    """
    When we execute command on clickhouse01
    """
    chadmin table delete --detached test_db test_table
    """
    Then we get response contains
    """
    """
    When we execute command on clickhouse01
    """
    ls -l /var/lib/clickhouse/data/test_db/
    """
    Then we get response
    """
    total 0
    """