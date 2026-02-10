Feature: chadmin table schema-diff command

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario: Compare MergeTree table with its metadata file
    When we execute query on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    """
    And we execute query on clickhouse01
    """
    CREATE TABLE test_db.t1
    (
        `key` Int64,
        `someCol` String,
        `eventTime` DateTime
    )
    ENGINE = MergeTree
    ORDER BY key
    SETTINGS index_granularity = 8192;
    """
    When we execute command on clickhouse01
    """
    chadmin table schema-diff test_db.t1 /var/lib/clickhouse/metadata/test_db/t1.sql --no-color --ignore-uuid
    """
    Then we get response contains
    """
    Schemas are identical
    """

  Scenario: Compare ReplicatedMergeTree table with its metadata file
    When we execute query on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    """
    And we execute query on clickhouse01
    """
    CREATE TABLE test_db.replicated_t1 ON CLUSTER '{cluster}'
    (
        `id` Int64,
        `name` String,
        `created_at` DateTime
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_db/replicated_t1', '{replica}')
    ORDER BY id
    SETTINGS index_granularity = 8192;
    """
    When we execute command on clickhouse01
    """
    chadmin table schema-diff test_db.replicated_t1 /var/lib/clickhouse/metadata/test_db/replicated_t1.sql --no-color --ignore-uuid
    """
    Then we get response contains
    """
    Schemas are identical
    """

  Scenario: Verify schema from ClickHouse has no escaped newlines
    When we execute query on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    """
    And we execute query on clickhouse01
    """
    CREATE TABLE test_db.t3
    (
        `key` Int64,
        `someCol` String,
        `eventTime` DateTime
    )
    ENGINE = MergeTree
    ORDER BY key;
    """
    When we execute command on clickhouse01
    """
    chadmin table schema-diff test_db.t3 test_db.t3 --no-color
    """
    Then we get response contains
    """
    Schemas are identical
    """
    And we get response does not contain
    """
    \n
    """

  Scenario: Compare table with different columns shows difference
    When we execute query on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    """
    And we execute query on clickhouse01
    """
    CREATE TABLE test_db.t4
    (
        `id` Int64,
        `name` String
    )
    ENGINE = MergeTree
    ORDER BY id;
    """
    And we create file "/tmp/t4_modified.sql" on clickhouse01 with content
    """
    CREATE TABLE test_db.t4
    (
        `id` Int64,
        `name` String,
        `age` UInt32
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS index_granularity = 8192
    """
    When we execute command on clickhouse01
    """
    chadmin table schema-diff test_db.t4 /tmp/t4_modified.sql --no-color
    """
    Then we get response contains
    """
    age
    """

  Scenario: Compare tables with different engines using ignore-engine flag
    When we execute query on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    """
    And we execute query on clickhouse01
    """
    CREATE TABLE test_db.t5
    (
        `id` Int64,
        `data` String
    )
    ENGINE = MergeTree
    ORDER BY id;
    """
    And we create file "/tmp/t5_replacing.sql" on clickhouse01 with content
    """
    CREATE TABLE test_db.t5
    (
        `id` Int64,
        `data` String
    )
    ENGINE = ReplacingMergeTree
    ORDER BY id
    SETTINGS index_granularity = 8192
    """
    When we execute command on clickhouse01
    """
    chadmin table schema-diff test_db.t5 /tmp/t5_replacing.sql --no-color --ignore-engine
    """
    Then we get response contains
    """
    Schemas are identical
    """