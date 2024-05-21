Feature: chadmin delete detached table commands

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_drop_detach_db;
    """

  Scenario: Drop detached table from local disk
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE IF NOT EXISTS test_drop_detach_db.test_table_local (n Int32)
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO test_drop_detach_db.test_table_local (n) SELECT number FROM system.numbers LIMIT 10;
    DETACH TABLE test_drop_detach_db.test_table_local SYNC;
    """
    When we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/data/test_drop_detach_db/
    """
    Then we get response
    """
    test_table_local
    """
    When we execute command on clickhouse01
    """
    chadmin table delete --detached test_drop_detach_db test_table_local
    """
    Then we get response contains
    """
    """
    When we execute command on clickhouse01
    """
    ls -l /var/lib/clickhouse/data/test_drop_detach_db/
    """
    Then we get response
    """
    total 0
    """

  Scenario: Drop permanently detached table
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE IF NOT EXISTS test_drop_detach_db.test_table_local (n Int32)
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO test_drop_detach_db.test_table_local (n) SELECT number FROM system.numbers LIMIT 10;
    DETACH TABLE test_drop_detach_db.test_table_local PERMANENTLY SYNC;
    """
    When we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/data/test_drop_detach_db/
    """
    Then we get response
    """
    test_table_local
    """
    When we execute command on clickhouse01
    """
    chadmin table delete --detached test_drop_detach_db test_table_local
    """
    Then we get response contains
    """
    """
    When we execute command on clickhouse01
    """
    ls -l /var/lib/clickhouse/data/test_drop_detach_db/
    """
    Then we get response
    """
    total 0
    """

  Scenario: Drop detached table from object_storage
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE IF NOT EXISTS test_drop_detach_db.test_table_object_storage (n Int32)
    ENGINE = MergeTree
    ORDER BY n
    SETTINGS storage_policy = 'object_storage';

    INSERT INTO test_drop_detach_db.test_table_object_storage (n) SELECT number FROM system.numbers LIMIT 10;
    DETACH TABLE test_drop_detach_db.test_table_object_storage SYNC;
    """
    Then S3 contains greater than 0 objects

    When we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/data/test_drop_detach_db/
    """
    Then we get response
    """
    test_table_object_storage
    """
    When we execute command on clickhouse01
    """
    chadmin table delete --detached test_drop_detach_db test_table_object_storage
    """
    Then we get response contains
    """
    """
    When we execute command on clickhouse01
    """
    ls -l /var/lib/clickhouse/data/test_drop_detach_db/
    """
    Then we get response
    """
    total 0
    """
    Then S3 contains 0 objects