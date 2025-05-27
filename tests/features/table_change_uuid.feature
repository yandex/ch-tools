Feature: chadmin table change and check-uuid-equal

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

  @require_version_24.8
  Scenario: Change uuid for MergeTree
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
    When we execute command on clickhouse01
    """
    chadmin table change -d non_repl_db -t foo --uuid '123e4567-e89b-12d3-a456-426614174000'
    """
    Then it completes successfully
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
    When we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.foo VALUES (45)
    """
    When we execute query on clickhouse01
    """
    SELECT * FROM non_repl_db.foo ORDER BY a FORMAT Values
    """
    Then we get response
    """
    (42),(45)
    """

  @require_version_24.8
  Scenario: Failed change uuid for ReplicatedMergeTree that different from table_shared_id
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
    ENGINE = ReplicatedMergeTree('/clickhouse/foo', '{replica}')
    ORDER BY a
    """
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.foo VALUES (42)
    """
    When we try to execute command on clickhouse01
    """
    chadmin table change -d non_repl_db -t foo --uuid '123e4567-e89b-12d3-a456-426614174000'
    """
    Then it fails with response contains
    """
    Changing uuid for ReplicatedMergeTree that different from table_shared_id path was not allowed
    """

  @require_version_24.8
  Scenario: Failed change uuid for ReplicatedMergeTree on first host
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
    ENGINE = ReplicatedMergeTree('/clickhouse/foo', '{replica}')
    ORDER BY a
    """
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.foo VALUES (42)
    """
    When we try to execute command on clickhouse01
    """
    chadmin table change -d non_repl_db -t foo --uuid "$(chadmin zookeeper get /clickhouse/foo/table_shared_id)"
    """
    Then it fails with response contains
    """
    Table has already had uuid
    """

  @require_version_24.8
  Scenario: Change uuid for ReplicatedMergeTree that created with ON CLUSTER
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
    ENGINE = ReplicatedMergeTree('/clickhouse/foo/', '{replica}')
    ORDER BY a
    """
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.foo VALUES (42)
    """
    Then check uuid table foo equal to table_shared_id by path /clickhouse/foo/table_shared_id on clickhouse01
    When we try to execute command on clickhouse01
    """
    chadmin table change -d non_repl_db -t foo --uuid "$(chadmin zookeeper get /clickhouse/foo/table_shared_id)"
    """
    Then it fails with response contains
    """
    Table has already had uuid
    """
    Then check uuid table foo equal to table_shared_id by path /clickhouse/foo/table_shared_id on clickhouse02
    When we try to execute command on clickhouse02
    """
    chadmin table change -d non_repl_db -t foo --uuid "$(chadmin zookeeper get /clickhouse/foo/table_shared_id)"
    """
    Then it fails with response contains
    """
    Table has already had uuid
    """

  @require_version_24.8
  Scenario: Change uuid for ReplicatedMergeTree
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
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.foo VALUES (42)
    """
    Then check uuid table foo equal to table_shared_id by path /clickhouse/foo/table_shared_id on clickhouse01
    When we try to execute command on clickhouse01
    """
    chadmin table change -d non_repl_db -t foo --uuid "$(chadmin zookeeper get /clickhouse/foo/table_shared_id)"
    """
    Then it fails with response contains
    """
    Table has already had uuid
    """

    When we execute command on clickhouse02
    """
    chadmin table change -d non_repl_db -t foo --uuid "$(chadmin zookeeper get /clickhouse/foo/table_shared_id)"
    """
    Then it completes successfully
    When we execute command on clickhouse02
    """
    supervisorctl restart clickhouse-server
    """
    When we sleep for 10 seconds
    Then check uuid table foo equal to table_shared_id by path /clickhouse/foo/table_shared_id on clickhouse02

    When we execute query on clickhouse02
    """
    SELECT * FROM non_repl_db.foo ORDER BY a FORMAT Values
    """
    Then we get response
    """
    (42)
    """
    When we execute query on clickhouse02
    """
    SELECT is_readonly FROM system.replicas WHERE database='non_repl_db' AND table='foo' FORMAT Values
    """
    Then we get response
    """
    (0)
    """
    When we execute query on clickhouse02
    """
    INSERT INTO non_repl_db.foo VALUES (45)
    """
    When we execute query on clickhouse02
    """
    SELECT * FROM non_repl_db.foo ORDER BY a FORMAT Values
    """
    Then we get response
    """
    (42),(45)
    """
    When we execute query on clickhouse01
    """
    SYSTEM SYNC REPLICA non_repl_db.foo
    """
    When we execute query on clickhouse01
    """
    SELECT * FROM non_repl_db.foo ORDER BY a FORMAT Values
    """
    Then we get response
    """
    (42),(45)
    """

  @require_version_24.8
  Scenario: Failed change uuid for ReplicatedMergeTree with uuid macros
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
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.foo VALUES (42)
    """
    When we try to execute command on clickhouse01
    """
    chadmin table change -d non_repl_db -t foo --uuid '123e4567-e89b-12d3-a456-426614174000'
    """
    Then it fails with response contains
    """
    Changing uuid for ReplicatedMergeTree that contains macros uuid in replica path was not allowed. replica_path=/clickhouse/foo/{uuid}
    """


  @require_version_24.8
  Scenario: Change uuid for ReplicatedMergeTree with ALTER
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
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.foo VALUES (42)
    """
    When we execute query on clickhouse02
    """
    SYSTEM STOP PULLING REPLICATION LOG non_repl_db.foo
    """
    And we execute query on clickhouse01
    """
    ALTER TABLE non_repl_db.foo ADD COLUMN b String DEFAULT 'b_one'
    """
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.foo VALUES (43, DEFAULT)
    """
    When we execute command on clickhouse02
    """
    chadmin table change -d non_repl_db -t foo --uuid "$(chadmin zookeeper get /clickhouse/foo/table_shared_id)"
    """
    Then it completes successfully
    When we execute query on clickhouse02
    """
    SYSTEM START PULLING REPLICATION LOG non_repl_db.foo
    """
    When we execute command on clickhouse02
    """
    supervisorctl restart clickhouse-server
    """
    When we sleep for 10 seconds
    Then check uuid table foo equal to table_shared_id by path /clickhouse/foo/table_shared_id on clickhouse02

    When we execute query on clickhouse02
    """
    SYSTEM SYNC REPLICA non_repl_db.foo
    """

    When we execute query on clickhouse02
    """
    SELECT * FROM non_repl_db.foo ORDER BY a FORMAT Values
    """
    Then we get response
    """
    (42,'b_one'),(43,'b_one')
    """
    When we execute query on clickhouse02
    """
    SELECT is_readonly FROM system.replicas WHERE database='non_repl_db' AND table='foo' FORMAT Values
    """
    Then we get response
    """
    (0)
    """
    When we execute query on clickhouse02
    """
    INSERT INTO non_repl_db.foo VALUES (45, DEFAULT)
    """
    When we execute query on clickhouse02
    """
    SELECT * FROM non_repl_db.foo ORDER BY a FORMAT Values
    """
    Then we get response
    """
    (42,'b_one'),(43,'b_one'),(45,'b_one')
    """
    When we execute query on clickhouse01
    """
    SYSTEM SYNC REPLICA non_repl_db.foo
    """
    When we execute query on clickhouse01
    """
    SELECT * FROM non_repl_db.foo ORDER BY a FORMAT Values
    """
    Then we get response
    """
    (42,'b_one'),(43,'b_one'),(45,'b_one')
    """

  @require_version_24.8
  Scenario: Change uuid for ReplicatedMergeTree with different table_shared_id
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
    ENGINE = ReplicatedMergeTree('/clickhouse/test_table1/', '{replica}')
    ORDER BY a
    """
    When we execute query on clickhouse02
    """
    CREATE TABLE non_repl_db.test_table1
    (
        `a` Int
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/test_table1/', '{replica}')
    ORDER BY a
    """
    And we execute query on clickhouse01
    """
    INSERT INTO non_repl_db.test_table1 VALUES (42)
    """
    When we execute command on clickhouse02
    """
    chadmin zookeeper update /clickhouse/test_table1/table_shared_id 864684af-db8a-4ba1-a7a9-0ffd30b3a623
    """
    Then it completes successfully

    When we execute command on clickhouse01
    """
    chadmin table change -d non_repl_db -t test_table1 --zk
    """
    Then it completes successfully
    When we execute command on clickhouse01
    """
    supervisorctl restart clickhouse-server
    """
    When we execute command on clickhouse02
    """
    chadmin table change -d non_repl_db -t test_table1 --zk
    """
    Then it completes successfully
    When we execute command on clickhouse02
    """
    supervisorctl restart clickhouse-server
    """
    When we sleep for 10 seconds

    When we execute query on clickhouse01
    """
    SELECT uuid FROM system.tables WHERE table='test_table1' FORMAT Values
    """
    Then we get response
    """
    ('864684af-db8a-4ba1-a7a9-0ffd30b3a623')
    """

    When we execute query on clickhouse02
    """
    SELECT uuid FROM system.tables WHERE table='test_table1' FORMAT Values
    """
    Then we get response
    """
    ('864684af-db8a-4ba1-a7a9-0ffd30b3a623')
    """

    When we execute query on clickhouse02
    """
    INSERT INTO non_repl_db.test_table1 VALUES (45)
    """

    When we execute query on clickhouse02
    """
    SELECT * FROM non_repl_db.test_table1 ORDER BY a FORMAT Values
    """
    Then we get response
    """
    (42),(45)
    """
    When we sleep for 5 seconds
    When we execute query on clickhouse01
    """
    SELECT * FROM non_repl_db.test_table1 ORDER BY a FORMAT Values
    """
    Then we get response
    """
    (42),(45)
    """
