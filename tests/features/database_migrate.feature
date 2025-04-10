Feature: chadmin database migrate command

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    
  @require_version_24.8
  Scenario: Migrate empty database in host
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

  @require_version_24.8
  Scenario: Migrate non exists database
    When we try to execute command on clickhouse01
    """
    chadmin database migrate -d non_exists_db 
    """
    Then it fails with response contains
    """
    Database non_exists_db does not exists, skip migrating
    """

  @require_version_24.8
  Scenario: Migrate database with MergeTree table in host
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
    ALTER TABLE non_repl_db.foo ADD COLUMN b String DEFAULT 'value'
    """
    When we execute query on clickhouse01
    """
    SELECT * FROM non_repl_db.foo FORMAT Values
    """
    Then we get response
    """
    (42,'value')
    """

 @require_version_24.8
  Scenario: Migrate empty database in cluster
    When we execute query on clickhouse01
    """
    CREATE DATABASE non_repl_db ON CLUSTER '{cluster}';
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
    When we execute command on clickhouse02
    """
    chadmin database migrate -d non_repl_db 
    """
    # restart
    When we execute command on clickhouse02
    """
    supervisorctl restart clickhouse-server
    """
    When we sleep for 10 seconds
    
    And we execute query on clickhouse02
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
    When we execute query on clickhouse02
    """
    SELECT name FROM system.tables WHERE database='non_repl_db'
    """
    Then we get response
    """
    bar2
    """

  @require_version_24.8
  Scenario: Migrate database with MergeTree table in cluster
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
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.foo VALUES (42)
    """
    And we execute query on clickhouse02
    """
    CREATE TABLE non_repl_db.foo
    (
        `a` Int
    )
    ENGINE = MergeTree
    ORDER BY a
    """
    And we execute query on clickhouse02
    """
    INSERT INTO non_repl_db.foo VALUES (42)
    """
    And we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db 
    """
    When we execute query on clickhouse01
    """
    SELECT name FROM system.databases ORDER BY name FORMAT Values
    """
    Then we get response
    """
    ('INFORMATION_SCHEMA'),('default'),('information_schema'),('non_repl_db'),('system')
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
    SELECT * FROM non_repl_db.foo FORMAT Values
    """
    Then we get response
    """
    (42)
    """

    When we execute command on clickhouse02
    """
    chadmin database migrate -d non_repl_db 
    """
    # restart
    When we execute command on clickhouse02
    """
    supervisorctl restart clickhouse-server
    """
    When we sleep for 10 seconds

    When we execute query on clickhouse02
    """
    SELECT name FROM system.databases ORDER BY name FORMAT Values
    """
    Then we get response
    """
    ('INFORMATION_SCHEMA'),('default'),('information_schema'),('non_repl_db'),('system')
    """
    When we execute query on clickhouse02
    """
    SELECT engine FROM system.databases WHERE database='non_repl_db'
    """
    Then we get response
    """
    Replicated
    """
    When we execute query on clickhouse02
    """
    SELECT * FROM non_repl_db.foo FORMAT Values
    """
    Then we get response
    """
    (42)
    """

    When we execute query on clickhouse01
    """
    ALTER TABLE non_repl_db.foo ADD COLUMN b String DEFAULT 'value'
    """

    When we execute query on clickhouse01
    """
    SELECT * FROM non_repl_db.foo FORMAT Values
    """
    Then we get response
    """
    (42,'value')
    """
    When we execute query on clickhouse02
    """
    SELECT * FROM non_repl_db.foo FORMAT Values
    """
    Then we get response
    """
    (42,'value')
    """