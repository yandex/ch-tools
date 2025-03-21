Feature: chadmin table change uuid

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  @require_version_24.8
  Scenario: Simple change uuid
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
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.foo VALUES (42)
    """
    And we execute command on clickhouse01
    """
    chadmin table change -d non_repl_db -t foo --uuid '123e4567-e89b-12d3-a456-426614174000'
    """
    When we execute command on clickhouse01
    """
    supervisorctl restart clickhouse-server
    """
    When we sleep for 10 seconds

    When we execute query on clickhouse01
    """
    SELECT uuid FROM system.tables WHERE database='non_repl_db' AND table='foo' FORMAT Values
    """
    Then we get response
    """
    ('123e4567-e89b-12d3-a456-426614174000')
    """
    When we execute query on clickhouse01
    """
    SELECT * FROM non_repl_db.foo
    """
    Then we get response
    """
    42
    """
    When we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/store/123/123e4567-e89b-12d3-a456-426614174000/ | wc -l
    """
    Then we get response
    """
    3
    """