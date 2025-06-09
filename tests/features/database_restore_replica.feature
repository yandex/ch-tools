Feature: chadmin database restore-replica command

  Background:
    Given default configuration
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02


  @require_version_24.8
  Scenario Outline: Restore database replica
    When we execute query on clickhouse01
    """
    CREATE DATABASE repl_db ON CLUSTER '{cluster}' 
    ENGINE=Replicated(<replica_path>, '{shard}', '{replica}');
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE repl_db.foo
    (
        `a` Int
    )
    ENGINE = ReplicatedMergeTree
    ORDER BY a
    """
    And we execute query on clickhouse01
    """
    INSERT INTO repl_db.foo VALUES (42)
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper delete <replica_path>
    """
    Then it completes successfully

    When we execute command on clickhouse01
    """
    chadmin database restore-replica -d repl_db
    """
    Then it completes successfully

    When we execute query on clickhouse01
    """
    DETACH DATABASE repl_db
    """
    When we execute query on clickhouse01
    """
    ATTACH DATABASE repl_db
    """

    When we execute query on clickhouse02
    """
    DETACH DATABASE repl_db
    """
    When we execute query on clickhouse02
    """
    ATTACH DATABASE repl_db
    """

    When we execute query on clickhouse01
    """
    SELECT table FROM system.tables WHERE database='repl_db' FORMAT Values
    """
    Then we get response
    """
    ('foo')
    """

    When we execute query on clickhouse01
    """
    CREATE TABLE repl_db.bar
    (
        `a` Int
    )
    ENGINE = ReplicatedMergeTree
    ORDER BY a
    """
    And we execute query on clickhouse01
    """
    INSERT INTO repl_db.bar VALUES (43)
    """

    When we execute query on clickhouse01
    """
    SELECT * FROM repl_db.foo FORMAT Values
    """
    Then we get response
    """
    (42)
    """
    When we execute query on clickhouse01
    """
    SELECT * FROM repl_db.bar FORMAT Values
    """
    Then we get response
    """
    (43)
    """

    When we execute query on clickhouse02
    """
    SYSTEM SYNC REPLICA repl_db.bar
    """
    When we execute query on clickhouse02
    """
    SELECT * FROM repl_db.foo FORMAT Values
    """
    Then we get response
    """
    (42)
    """
    When we execute query on clickhouse02
    """
    SELECT * FROM repl_db.bar FORMAT Values
    """
    Then we get response
    """
    (43)
    """
  Examples:
    | replica_path                                                 |
    | '/clickhouse/repl_db'                                        |
    | '/awesome_clickhouse/repl_db'                                |

  @require_version_24.8
  Scenario: Restore empty database
    When we execute query on clickhouse01
    """
    CREATE DATABASE repl_db ON CLUSTER '{cluster}' 
    ENGINE=Replicated('/clickhouse/repl_db', '{shard}', '{replica}');
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper delete '/clickhouse/repl_db'
    """
    Then it completes successfully

    When we execute command on clickhouse01
    """
    chadmin database restore-replica -d repl_db
    """
    Then it completes successfully

    When we execute query on clickhouse01
    """
    DETACH DATABASE repl_db
    """
    When we execute query on clickhouse01
    """
    ATTACH DATABASE repl_db
    """

    When we execute command on clickhouse02
    """
    chadmin database restore-replica -d repl_db
    """
    Then it completes successfully
    When we execute query on clickhouse02
    """
    DETACH DATABASE repl_db
    """
    When we execute query on clickhouse02
    """
    ATTACH DATABASE repl_db
    """

    When we execute query on clickhouse01
    """
    CREATE TABLE repl_db.bar
    (
        `a` Int
    )
    ENGINE = ReplicatedMergeTree
    ORDER BY a
    """
    And we execute query on clickhouse01
    """
    INSERT INTO repl_db.bar VALUES (43)
    """

    When we execute query on clickhouse01
    """
    SELECT * FROM repl_db.bar FORMAT Values
    """
    Then we get response
    """
    (43)
    """
    When we execute query on clickhouse02
    """
    SYSTEM SYNC REPLICA repl_db.bar
    """
    When we execute query on clickhouse02
    """
    SELECT * FROM repl_db.bar FORMAT Values
    """
    Then we get response
    """
    (43)
    """

  @require_version_24.8
  Scenario: Failed restore replica Atomic database
    When we execute query on clickhouse01
    """
    CREATE DATABASE repl_db ON CLUSTER '{cluster}'
    """
    When we try to execute command on clickhouse01
    """
    chadmin database restore-replica -d repl_db
    """
    Then it fails with response contains
    """
    Database repl_db is not Replicated, stop restore
    """
    