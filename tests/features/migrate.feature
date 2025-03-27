Feature: migrate engine

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  @require_version_24.8
  Scenario: Check table uuid in cluster with 1 host
    When we execute query on clickhouse01
    """
    CREATE DATABASE non_repl_db;
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE non_repl_db.foo
    (
        `a` Int
    )
    ENGINE = MergeTree
    ORDER BY a
    """
    And we execute command on clickhouse01
    """
    chadmin table check-uuid-equal -d non_repl_db -t foo
    """
    Then it completes successfully

  @require_version_24.8
  Scenario: Failed check table uuid in cluster
    When we execute query on clickhouse01
    """
    CREATE DATABASE non_repl_db ON CLUSTER '{cluster}';
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE non_repl_db.foo
    (
        `a` Int
    )
    ENGINE = MergeTree
    ORDER BY a
    """
    When we execute query on clickhouse02
    """
    CREATE TABLE non_repl_db.foo
    (
        `a` Int
    )
    ENGINE = MergeTree
    ORDER BY a
    """
    When we try to execute command on clickhouse01
    """
    chadmin table check-uuid-equal -d non_repl_db -t foo
    """
    Then it fails with response contains
    """
    Table foo has different uuid
    """
    When we try to execute command on clickhouse02
    """
    chadmin table check-uuid-equal -d non_repl_db -t foo
    """
    Then it fails with response contains
    """
    Table foo has different uuid
    """
    
  @require_version_24.8
  Scenario: Check uuid for ReplicatedMergeTree
    When we execute query on clickhouse01
    """
    CREATE DATABASE non_repl_db ON CLUSTER 'cluster';
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE non_repl_db.foo
    (
        `a` Int
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/foo/', '{replica}')
    ORDER BY a
    """
    When we execute query on clickhouse02
    """
    CREATE TABLE non_repl_db.foo
    (
        `a` Int
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/foo/', '{replica}')
    ORDER BY a
    """
    When we try to execute command on clickhouse01
    """
    chadmin table check-uuid-equal -d non_repl_db -t foo
    """
    Then it fails with response contains
    """
    Table foo has different uuid
    """
    When we try to execute command on clickhouse02
    """
    chadmin table check-uuid-equal -d non_repl_db -t foo
    """
    Then it fails with response contains
    """
    Table foo has different uuid
    """

  @require_version_24.8
  Scenario: Check uuid for ReplicatedMergeTree with uuid macros
    When we execute query on clickhouse01
    """
    CREATE DATABASE non_repl_db ON CLUSTER 'cluster';
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE non_repl_db.foo
    ON CLUSTER 'cluster'
    (
        `a` Int
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/foo/{uuid}', '{replica}')
    ORDER BY a
    """
    When we execute command on clickhouse01
    """
    chadmin table check-uuid-equal -d non_repl_db -t foo
    """
    Then it completes successfully
    When we execute command on clickhouse02
    """
    chadmin table check-uuid-equal -d non_repl_db -t foo
    """
    Then it completes successfully
