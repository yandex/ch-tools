Feature: chadmin database migrate command

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    
  @require_version_25.8
  Scenario: Migrate empty database in host
    When we execute query on clickhouse01
    """
    CREATE DATABASE non_repl_db;
    """
    And we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db --engine Replicated
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

  @require_version_25.8
  Scenario: Migrate non exists database
    When we try to execute command on clickhouse01
    """
    chadmin database migrate -d non_exists_db --engine Replicated
    """
    Then it fails with response contains
    """
    Database non_exists_db does not exists, skip migrating
    """

  @require_version_less_than_25.8
  Scenario: Migrate database on unsupported version
    When we execute query on clickhouse01
    """
    CREATE DATABASE non_repl_db;
    """
    When we try to execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db --engine Replicated
    """
    Then it fails with response contains
    """
    Migration requires ClickHouse version 25.8 or above
    """

  @require_version_25.8
  Scenario Outline: Migrate database with different tables in host created by hosts
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
    ENGINE = <table_engine>
    ORDER BY a
    """
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.foo VALUES (42)
    """

    And we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
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
  Examples:
    | table_engine                                                 |
    | MergeTree                                                    |
    | ReplicatedMergeTree('/clickhouse/foo', '{replica}')          |

  @require_version_25.8
  Scenario Outline: Migrate database with different tables in host created on cluster
    When we execute query on clickhouse01
    """
    CREATE DATABASE non_repl_db;
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE non_repl_db.foo
    ON CLUSTER '{cluster}'
    (
        `a` Int
    )
    ENGINE = <table_engine>
    ORDER BY a
    """
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.foo VALUES (42)
    """

    And we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
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
  Examples:
    | table_engine                                                 |
    | MergeTree                                                    |
    | ReplicatedMergeTree('/clickhouse/foo', '{replica}')          |

  @require_version_25.8
  Scenario: Migrate empty database in cluster
    When we execute query on clickhouse01
    """
    CREATE DATABASE non_repl_db ON CLUSTER '{cluster}';
    """
    And we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
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
    chadmin database migrate -d non_repl_db -e Replicated
    """

    When we execute query on clickhouse02
    """
    SYSTEM SYNC DATABASE REPLICA non_repl_db
    """
    
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

  @require_version_25.8
  Scenario: Migrate database with MergeTree table by hosts
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
    chadmin database migrate -d non_repl_db -e Replicated
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
    chadmin database migrate -d non_repl_db -e Replicated
    """

    When we execute query on clickhouse02
    """
    SYSTEM SYNC DATABASE REPLICA non_repl_db
    """

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

  @require_version_25.8
  Scenario: Migrate database with ReplicatedMergeTree table with stopped first replica
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
    ENGINE = ReplicatedMergeTree('/clickhouse/foo', '{replica}')
    ORDER BY a
    """
    And we execute query on clickhouse02
    """
    CREATE TABLE non_repl_db.foo
    (
        `a` Int
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/foo', '{replica}')
    ORDER BY a
    """
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.foo VALUES (42)
    """
    And we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """
    Then it completes successfully

    When we execute command on clickhouse01
    """
    supervisorctl stop clickhouse-server
    """

    When we execute command on clickhouse02
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """
    Then it completes successfully

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

  @require_version_25.8
  Scenario Outline: Migrate database with ReplicatedMergeTree table createed by hosts
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
    ENGINE = <table_engine>
    ORDER BY a
    """
    And we execute query on clickhouse02
    """
    CREATE TABLE non_repl_db.foo
    (
        `a` Int
    )
    ENGINE = <table_engine>
    ORDER BY a
    """
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.foo VALUES (42)
    """
    And we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
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
    chadmin database migrate -d non_repl_db -e Replicated
    """

    When we execute query on clickhouse02
    """
    SYSTEM SYNC DATABASE REPLICA non_repl_db
    """

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

    When we execute query on clickhouse02
    """
    SELECT zookeeper_path FROM system.replicas WHERE table='foo' FORMAT Values
    """
    Then we get response
    """
    ('<zookeeper_path>')
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
    When we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.foo VALUES (43, 'value2')
    """
    When we execute query on clickhouse02
    """
    SYSTEM SYNC REPLICA non_repl_db.foo
    """
    When we execute query on clickhouse01
    """
    SELECT * FROM non_repl_db.foo ORDER BY a FORMAT Values
    """
    Then we get response
    """
    (42,'value'),(43,'value2')
    """
    When we execute query on clickhouse02
    """
    SELECT * FROM non_repl_db.foo ORDER BY a FORMAT Values
    """
    Then we get response
    """
    (42,'value'),(43,'value2')
    """
    When we execute query on clickhouse01
    """
    DROP DATABASE non_repl_db ON CLUSTER '{cluster}';
    """
  Examples:
      | table_engine                                                 | zookeeper_path          |
      | ReplicatedMergeTree('/clickhouse/foo/{shard}', '{replica}')  | /clickhouse/foo/shard1  |
      | ReplicatedMergeTree('/clickhouse/foo', '{replica}')          | /clickhouse/foo         |


  @require_version_25.8
  Scenario Outline: Migrate database with ReplicatedMergeTree table created on cluster
    When we execute query on clickhouse01
    """
    CREATE DATABASE non_repl_db ON CLUSTER '{cluster}';
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE non_repl_db.foo
    ON CLUSTER '{cluster}'
    (
        `a` Int
    )
    ENGINE = <table_engine>
    ORDER BY a
    """
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.foo VALUES (42)
    """
    And we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
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
    chadmin database migrate -d non_repl_db -e Replicated
    """

    When we execute query on clickhouse02
    """
    SYSTEM SYNC DATABASE REPLICA non_repl_db
    """

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

    When we execute query on clickhouse02
    """
    SELECT zookeeper_path FROM system.replicas WHERE table='foo' FORMAT Values
    """
    Then we get response
    """
    ('<zookeeper_path>')
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
    When we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.foo VALUES (43, 'value2')
    """
    When we execute query on clickhouse02
    """
    SYSTEM SYNC REPLICA non_repl_db.foo
    """
    When we execute query on clickhouse01
    """
    SELECT * FROM non_repl_db.foo ORDER BY a FORMAT Values
    """
    Then we get response
    """
    (42,'value'),(43,'value2')
    """
    When we execute query on clickhouse02
    """
    SELECT * FROM non_repl_db.foo ORDER BY a FORMAT Values
    """
    Then we get response
    """
    (42,'value'),(43,'value2')
    """
    When we execute query on clickhouse01
    """
    DROP DATABASE non_repl_db ON CLUSTER '{cluster}';
    """
  Examples:
      | table_engine                                                 | zookeeper_path          |
      | ReplicatedMergeTree('/clickhouse/foo/{shard}', '{replica}')  | /clickhouse/foo/shard1  |
      | ReplicatedMergeTree('/clickhouse/foo', '{replica}')          | /clickhouse/foo         |

  @require_version_25.8
  Scenario: Migrate database with Distributed table created by hosts
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
    ENGINE = ReplicatedMergeTree('/clickhouse/foo', '{replica}')
    ORDER BY a
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE non_repl_db.dist_foo
    AS non_repl_db.foo
    ENGINE = Distributed('{cluster}', 'non_repl_db', 'foo', a);
    """
    When we execute query on clickhouse02
    """
    CREATE TABLE non_repl_db.foo
    (
        `a` Int
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/foo', '{replica}')
    ORDER BY a
    """
    When we execute query on clickhouse02
    """
    CREATE TABLE non_repl_db.dist_foo
    AS non_repl_db.foo
    ENGINE = Distributed('{cluster}', 'non_repl_db', 'foo', a);
    """
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.dist_foo VALUES (42)
    """
    And we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
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
    SELECT * FROM non_repl_db.dist_foo FORMAT Values
    """
    Then we get response
    """
    (42)
    """

    When we execute command on clickhouse02
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """

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
    SELECT * FROM non_repl_db.dist_foo FORMAT Values
    """
    Then we get response
    """
    (42)
    """

  @require_version_25.8
  Scenario: Migrate database with Distributed table created on cluster
    When we execute query on clickhouse01
    """
    CREATE DATABASE non_repl_db ON CLUSTER '{cluster}';
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE non_repl_db.foo
    ON CLUSTER '{cluster}'
    (
        `a` Int
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/foo', '{replica}')
    ORDER BY a
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE non_repl_db.dist_foo
    ON CLUSTER '{cluster}'
    AS non_repl_db.foo
    ENGINE = Distributed('{cluster}', 'non_repl_db', 'foo', a);
    """
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.dist_foo VALUES (42)
    """
    And we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
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
    SELECT * FROM non_repl_db.dist_foo FORMAT Values
    """
    Then we get response
    """
    (42)
    """

    When we execute command on clickhouse02
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """

    When we execute query on clickhouse02
    """
    SYSTEM SYNC DATABASE REPLICA non_repl_db
    """

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
    SELECT * FROM non_repl_db.dist_foo FORMAT Values
    """
    Then we get response
    """
    (42)
    """

  @require_version_25.8
  Scenario: Migrate database with VIEW by host
    When we execute query on clickhouse01
    """
    CREATE DATABASE non_repl_db;
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE non_repl_db.bar
    (
        `a` Int
    )
    ENGINE = MergeTree
    ORDER BY a
    """
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.bar VALUES (42)
    """
    When we execute query on clickhouse01
    """
    CREATE VIEW non_repl_db.foo
    AS SELECT * FROM non_repl_db.bar
    """
    When we execute query on clickhouse01
    """
    SELECT * FROM non_repl_db.foo
    """
    Then we get response
    """
    42
    """
    When we execute query on clickhouse01
    """
    SELECT name FROM system.tables WHERE database='non_repl_db' ORDER BY name FORMAT Values
    """
    Then we get response
    """
    ('bar'),('foo')
    """
    When we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """
    When we execute query on clickhouse01
    """
    SELECT * FROM non_repl_db.foo
    """
    Then we get response
    """
    42
    """

  @require_version_25.8
  Scenario: Migrate database with VIEW by hosts
    When we execute query on clickhouse01
    """
    CREATE DATABASE non_repl_db ON CLUSTER '{cluster}';
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE non_repl_db.bar
    (
        `a` Int
    )
    ENGINE = MergeTree
    ORDER BY a
    """
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.bar VALUES (42)
    """
    When we execute query on clickhouse01
    """
    CREATE VIEW non_repl_db.foo
    AS SELECT * FROM non_repl_db.bar
    """
    When we execute query on clickhouse02
    """
    CREATE TABLE non_repl_db.bar
    (
        `a` Int
    )
    ENGINE = MergeTree
    ORDER BY a
    """
    And we execute query on clickhouse02
    """
    INSERT INTO non_repl_db.bar VALUES (42)
    """
    When we execute query on clickhouse02
    """
    CREATE VIEW non_repl_db.foo
    AS SELECT * FROM non_repl_db.bar
    """
    When we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """
    When we execute query on clickhouse01
    """
    SELECT * FROM non_repl_db.foo
    """
    Then we get response
    """
    42
    """
    When we execute command on clickhouse02
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """
    
    When we execute query on clickhouse02
    """
    SYSTEM SYNC DATABASE REPLICA non_repl_db
    """
    
    When we execute query on clickhouse02
    """
    SELECT * FROM non_repl_db.foo
    """
    Then we get response
    """
    42
    """

  @require_version_25.8
  Scenario: Migrate database with VIEW by cluster
    When we execute query on clickhouse01
    """
    CREATE DATABASE non_repl_db ON CLUSTER '{cluster}';
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE non_repl_db.bar
    ON CLUSTER '{cluster}'
    (
        `a` Int
    )
    ENGINE = MergeTree
    ORDER BY a
    """
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.bar VALUES (42)
    """
    When we execute query on clickhouse01
    """
    CREATE VIEW non_repl_db.foo
    ON CLUSTER '{cluster}'
    AS SELECT * FROM non_repl_db.bar
    """
    And we execute query on clickhouse02
    """
    INSERT INTO non_repl_db.bar VALUES (42)
    """
    When we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """
    When we execute query on clickhouse01
    """
    SELECT * FROM non_repl_db.foo
    """
    Then we get response
    """
    42
    """
    When we execute command on clickhouse02
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """
    
    When we execute query on clickhouse02
    """
    SYSTEM SYNC DATABASE REPLICA non_repl_db
    """
    
    When we execute query on clickhouse02
    """
    SELECT * FROM non_repl_db.foo
    """
    Then we get response
    """
    42
    """

  @require_version_25.8
  Scenario: Migrate database with MATERIALIZED VIEW by host
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
    And we execute query on clickhouse01
    """
    CREATE MATERIALIZED VIEW non_repl_db.foo_mw TO non_repl_db.foo AS SELECT * FROM non_repl_db.foo
    """
    And we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
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
    SELECT * FROM non_repl_db.foo_mw FORMAT Values
    """
    Then we get response
    """
    (42)
    """

  @require_version_25.8
  Scenario: Migrate database with MATERIALIZED VIEW by hosts
    When we execute query on clickhouse01
    """
    CREATE DATABASE non_repl_db ON CLUSTER '{cluster}';
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE non_repl_db.foo
    ON CLUSTER '{cluster}'
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
    INSERT INTO non_repl_db.foo VALUES (43)
    """
    And we execute query on clickhouse01
    """
    CREATE MATERIALIZED VIEW non_repl_db.foo_mw
    TO non_repl_db.foo AS SELECT * FROM non_repl_db.foo
    """
    And we execute query on clickhouse02
    """
    CREATE MATERIALIZED VIEW non_repl_db.foo_mw
    TO non_repl_db.foo AS SELECT * FROM non_repl_db.foo
    """
    When we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
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
    SELECT * FROM non_repl_db.foo_mw FORMAT Values
    """
    Then we get response
    """
    (42)
    """
    When we execute command on clickhouse02
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """

    When we execute query on clickhouse02
    """
    SYSTEM SYNC DATABASE REPLICA non_repl_db
    """

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
    (43)
    """
    When we execute query on clickhouse02
    """
    SELECT * FROM non_repl_db.foo_mw FORMAT Values
    """
    Then we get response
    """
    (43)
    """

  @require_version_25.8
  Scenario: Migrate database with MATERIALIZED VIEW by cluster
    When we execute query on clickhouse01
    """
    CREATE DATABASE non_repl_db ON CLUSTER '{cluster}';
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE non_repl_db.foo
    ON CLUSTER '{cluster}'
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
    INSERT INTO non_repl_db.foo VALUES (43)
    """
    And we execute query on clickhouse01
    """
    CREATE MATERIALIZED VIEW non_repl_db.foo_mw
    ON CLUSTER '{cluster}'
    TO non_repl_db.foo AS SELECT * FROM non_repl_db.foo
    """
    When we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
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
    SELECT * FROM non_repl_db.foo_mw FORMAT Values
    """
    Then we get response
    """
    (42)
    """
    When we execute command on clickhouse02
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """

    When we execute query on clickhouse02
    """
    SYSTEM SYNC DATABASE REPLICA non_repl_db
    """

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
    (43)
    """
    When we execute query on clickhouse02
    """
    SELECT * FROM non_repl_db.foo_mw FORMAT Values
    """
    Then we get response
    """
    (43)
    """

  @require_version_25.8
  Scenario: Migrate database with different table_shared_id
    When we execute query on clickhouse01
    """
    CREATE DATABASE non_repl_db;
    """
    When we execute query on clickhouse02
    """
    CREATE DATABASE non_repl_db;
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE non_repl_db.test_table1
    (
        `a` Int
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/test_table1/{shard}', '{replica}')
    ORDER BY a
    """
    When we execute query on clickhouse02
    """
    CREATE TABLE non_repl_db.test_table1
    (
        `a` Int
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/test_table1/{shard}', '{replica}')
    ORDER BY a
    """
    When we execute command on clickhouse02
    """
    chadmin zookeeper update /clickhouse/test_table1/{shard}/table_shared_id 864684af-db8a-4ba1-a7a9-0ffd30b3a623
    """
    Then it completes successfully

    When we execute query on clickhouse02
    """
    CREATE TABLE non_repl_db.test_table2
    (
        `a` Int
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/test_table2/{shard}', '{replica}')
    ORDER BY a
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE non_repl_db.test_table2
    (
        `a` Int
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/test_table2/{shard}', '{replica}')
    ORDER BY a
    """
    When we execute command on clickhouse02
    """
    chadmin zookeeper update /clickhouse/test_table2/{shard}/table_shared_id a7434a27-e2e5-41d1-be33-6a1e19b33ab4
    """
    Then it completes successfully

    When we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.test_table1 VALUES (42)
    """
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.test_table2 VALUES (117)
    """

    When we execute command on clickhouse01
    """
    chadmin table change -d non_repl_db -t test_table1 --zk
    """
    Then it completes successfully
    When we execute command on clickhouse01
    """
    chadmin table change -d non_repl_db -t test_table2 --zk
    """
    Then it completes successfully
    When we execute command on clickhouse02
    """
    chadmin table change -d non_repl_db --all --zk
    """
    Then it completes successfully
    When we execute command on clickhouse01
    """
    supervisorctl restart clickhouse-server
    """
    When we execute command on clickhouse02
    """
    supervisorctl restart clickhouse-server
    """
    When we sleep for 10 seconds

    When we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """
    Then it completes successfully
    When we execute command on clickhouse02
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """
    Then it completes successfully

    When we execute query on clickhouse01
    """
    ALTER TABLE non_repl_db.test_table1 ADD COLUMN b String DEFAULT 'value'
    """
    When we execute query on clickhouse01
    """
    SELECT * FROM non_repl_db.test_table1 FORMAT Values
    """
    Then we get response
    """
    (42,'value')
    """

    When we execute query on clickhouse02
    """
    SELECT * FROM non_repl_db.test_table1 FORMAT Values
    """
    Then we get response
    """
    (42,'value')
    """

    When we execute query on clickhouse01
    """
    ALTER TABLE non_repl_db.test_table2 ADD COLUMN b String DEFAULT 'value'
    """
    When we execute query on clickhouse01
    """
    SELECT * FROM non_repl_db.test_table2 FORMAT Values
    """
    Then we get response
    """
    (117,'value')
    """

    When we execute query on clickhouse02
    """
    SELECT * FROM non_repl_db.test_table2 FORMAT Values
    """
    Then we get response
    """
    (117,'value')
    """
    When we execute command on clickhouse01
    """
    chadmin table check-uuid-equal -d non_repl_db -t test_table1
    """
    Then it completes successfully

    When we execute command on clickhouse01
    """
    chadmin table check-uuid-equal -d non_repl_db -t test_table2
    """
    Then it completes successfully


  @require_version_25.8
  Scenario: Migrate from Replicated to Atomic
    When we execute query on clickhouse01
    """
    CREATE DATABASE repl_db ENGINE=Replicated('/clickhouse/repl_db', '{shard}', '{replica}');
    """
    When we execute query on clickhouse02
    """
    CREATE DATABASE repl_db ENGINE=Replicated('/clickhouse/repl_db', '{shard}', '{replica}');
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE repl_db.test_table
    (
        `a` Int
    )
    ENGINE = ReplicatedMergeTree
    ORDER BY a
    """
    And we execute query on clickhouse01
    """
    INSERT INTO repl_db.test_table VALUES (42)
    """
    When we execute command on clickhouse01
    """
    chadmin database migrate -d repl_db -e Atomic
    """
    Then it completes successfully
    When we execute query on clickhouse01
    """
    SELECT engine FROM system.databases WHERE database='repl_db'
    """
    Then we get response
    """
    Atomic
    """
    When we execute query on clickhouse01
    """
    SELECT * FROM repl_db.test_table FORMAT Values
    """
    Then we get response
    """
    (42)
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper list /clickhouse/repl_db/
    """
    Then we get response
    """
    /clickhouse/repl_db/counter
    /clickhouse/repl_db/first_replica_database_name
    /clickhouse/repl_db/log
    /clickhouse/repl_db/logs_to_keep
    /clickhouse/repl_db/max_log_ptr
    /clickhouse/repl_db/metadata
    /clickhouse/repl_db/replicas
    """
    When we execute command on clickhouse02
    """
    chadmin database migrate -d repl_db -e Atomic --clean-zookeeper
    """
    Then it completes successfully
    When we execute query on clickhouse02
    """
    SELECT engine FROM system.databases WHERE database='repl_db'
    """
    Then we get response
    """
    Atomic
    """
    When we execute query on clickhouse02
    """
    SELECT * FROM repl_db.test_table FORMAT Values
    """
    Then we get response
    """
    (42)
    """

    When we try to execute command on clickhouse02
    """
    chadmin zookeeper list /clickhouse/repl_db
    """
    Then it fails with response contains
    """
    kazoo.exceptions.NoNodeError
    """

  @require_version_25.8
  Scenario: Restore table replica after migration to Replicated
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE non_repl_db ON CLUSTER '{cluster}';
    CREATE TABLE non_repl_db.test_table1
    ON CLUSTER '{cluster}'
    (
        `a` Int
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/test_table1/{shard}', '{replica}')
    ORDER BY a;
    INSERT INTO non_repl_db.test_table1 VALUES (42);
    """
    When we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """
    Then it completes successfully
    When we execute command on clickhouse02
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """
    Then it completes successfully
    When we execute command on clickhouse01
    """
    chadmin zookeeper delete '/clickhouse/test_table1/shard1/replicas/clickhouse01.ch_tools_test'
    """
    Then it completes successfully

    When we execute query on clickhouse01
    """
    DETACH TABLE non_repl_db.test_table1 PERMANENTLY SYNC
    """
    Then it completes successfully
    When we execute query on clickhouse01
    """
    ATTACH TABLE non_repl_db.test_table1
    """
    Then it completes successfully

    When we execute query on clickhouse01
    """
    SELECT is_readonly FROM system.replicas WHERE table='test_table1'
    """
    Then we get response
    """
    1
    """
    When we execute query on clickhouse01
    """
    SYSTEM RESTORE REPLICA non_repl_db.test_table1
    """
    Then it completes successfully
    When we execute query on clickhouse01
    """
    SELECT is_readonly FROM system.replicas WHERE table='test_table1'
    """
    Then we get response
    """
    0
    """
    When we execute query on clickhouse01
    """
    SELECT * FROM non_repl_db.test_table1
    """
    Then we get response
    """
    42
    """

    When we execute command on clickhouse01
    """
    chadmin zookeeper delete '/clickhouse/test_table1/'
    """
    Then it completes successfully
    When we execute query on clickhouse01
    """
    DETACH TABLE non_repl_db.test_table1 PERMANENTLY SYNC
    """
    Then it completes successfully
    When we execute query on clickhouse01
    """
    ATTACH TABLE non_repl_db.test_table1
    """
    Then it completes successfully

    When we execute query on clickhouse01
    """
    SELECT is_readonly FROM system.replicas WHERE table='test_table1'
    """
    Then we get response
    """
    1
    """
    When we execute query on clickhouse02
    """
    SELECT is_readonly FROM system.replicas WHERE table='test_table1'
    """
    Then we get response
    """
    1
    """

    When we execute query on clickhouse01
    """
    SYSTEM RESTORE REPLICA non_repl_db.test_table1
    """
    Then it completes successfully
    When we execute query on clickhouse01
    """
    SELECT is_readonly FROM system.replicas WHERE table='test_table1'
    """
    Then we get response
    """
    0
    """

    When we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.test_table1 VALUES (43);
    """

    When we execute query on clickhouse01
    """
    SELECT * FROM non_repl_db.test_table1 ORDER BY a FORMAT Values
    """
    Then we get response
    """
    (42),(43)
    """

    When we execute query on clickhouse02
    """
    SYSTEM RESTORE REPLICA non_repl_db.test_table1
    """
    Then it completes successfully
    When we execute query on clickhouse02
    """
    SELECT is_readonly FROM system.replicas WHERE table='test_table1'
    """
    Then we get response
    """
    0
    """
    When we execute query on clickhouse02
    """
    SYSTEM SYNC REPLICA non_repl_db.test_table1
    """
    When we execute query on clickhouse02
    """
    SELECT * FROM non_repl_db.test_table1 ORDER BY a FORMAT Values
    """
    Then we get response
    """
    (42),(43)
    """

  @require_version_25.8
  Scenario: Double migration
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE non_repl_db ON CLUSTER '{cluster}';
    CREATE TABLE non_repl_db.test_table1
    ON CLUSTER '{cluster}'
    (
        `a` Int
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/test_table1/{shard}', '{replica}')
    ORDER BY a;
    INSERT INTO non_repl_db.test_table1 VALUES (42);
    """
    When we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """
    Then it completes successfully
    When we execute command on clickhouse02
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """
    Then it completes successfully

    When we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Atomic
    """
    Then it completes successfully
    When we execute command on clickhouse02
    """
    chadmin database migrate -d non_repl_db -e Atomic
    """
    Then it completes successfully

    When we try to execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """
    Then it fails with response contains
    """
    Replica node '/clickhouse/non_repl_db/replicas/shard1|clickhouse01.ch_tools_test/digest' in ZooKeeper already exists and contains unexpected value
    """

  @require_version_25.8
  Scenario: Prohibited migration
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE non_repl_db ON CLUSTER '{cluster}';
    """
    When we try to execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Atomic
    """
    Then it fails with response contains
    """
    Database non_repl_db has engine Atomic. Migration to Atomic from Replicated only is supported.
    """
    When we execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """
    Then it completes successfully
    When we try to execute command on clickhouse01
    """
    chadmin database migrate -d non_repl_db -e Replicated
    """
    Then it fails with response contains
    """
    Database non_repl_db has engine Replicated. Migration to Replicated from Atomic only is supported.
    """
