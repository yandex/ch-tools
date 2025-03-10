Feature: chadmin database migrate command

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    
  Scenario: Migrate empty database
    When we execute query on clickhouse01
    """
    CREATE DATABASE non_repl_db;
    """
    And we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db 
    """
    When we execute query on clickhouse01
    """
    SELECT engine FROM system.databases WHERE database='non_repl_db'
    """
    Then we get response
    """
    Replicated
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE non_repl_db.bar2
    (
        `a` Int
    )
    ENGINE = MergeTree
    ORDER BY a
    """
    When we execute query on clickhouse01
    """
        SELECT name FROM system.tables WHERE database='non_repl_db'
    """
    Then we get response
    """
    bar2
    """
    