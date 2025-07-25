Feature: chadmin delete detached table commands

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_drop_db;
    """

  Scenario: Drop table(local) with position arguments
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE IF NOT EXISTS test_drop_db.test_table_local (n Int32)
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO test_drop_db.test_table_local (n) SELECT number FROM system.numbers LIMIT 10;

    CREATE TABLE IF NOT EXISTS test_drop_db.test_table_expected (n Int32)
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO test_drop_db.test_table_expected (n) SELECT number FROM system.numbers LIMIT 10;
    """

    Then save uuid table test_table_local in context on clickhouse01
    Then save uuid table test_table_expected in context on clickhouse01

    Then check local disk contains table test_table_local data in clickhouse01
    Then check local disk contains table test_table_expected data in clickhouse01

    When we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/data/test_drop_db/
    """
    Then we get response
    """
    test_table_expected
    test_table_local
    """
    When we execute command on clickhouse01
    """
    chadmin table delete test_drop_db test_table_local
    """
    Then we get response contains
    """
    """

    Then check table test_table_local not exists on local disk in clickhouse01
    Then check local disk contains table test_table_expected data in clickhouse01

    When we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/data/test_drop_db/
    """
    Then we get response
    """
    test_table_expected
    """

  Scenario: Drop tables(local) by pattern
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE IF NOT EXISTS test_drop_db.test_table_local1 (n Int32)
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO test_drop_db.test_table_local1 (n) SELECT number FROM system.numbers LIMIT 10;

    CREATE TABLE IF NOT EXISTS test_drop_db.test_table_local2 (n Int32)
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO test_drop_db.test_table_local2 (n) SELECT number FROM system.numbers LIMIT 10;

    CREATE TABLE IF NOT EXISTS test_drop_db.test_table_expected (n Int32)
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO test_drop_db.test_table_expected (n) SELECT number FROM system.numbers LIMIT 10;
    """

    Then save uuid table test_table_local1 in context on clickhouse01
    Then save uuid table test_table_local2 in context on clickhouse01
    Then save uuid table test_table_expected in context on clickhouse01

    Then check local disk contains table test_table_local1 data in clickhouse01
    Then check local disk contains table test_table_local2 data in clickhouse01
    Then check local disk contains table test_table_expected data in clickhouse01

    When we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/data/test_drop_db/
    """
    Then we get response
    """
    test_table_expected
    test_table_local1
    test_table_local2
    """
    When we execute command on clickhouse01
    """
    chadmin table delete -d test_drop_db -t test_table_local%
    """
    Then we get response contains
    """
    """

    Then check table test_table_local1 not exists on local disk in clickhouse01
    Then check table test_table_local2 not exists on local disk in clickhouse01
    Then check local disk contains table test_table_expected data in clickhouse01

    When we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/data/test_drop_db/
    """
    Then we get response
    """
    test_table_expected
    """

  Scenario: Drop tables(with object_storage) by pattern
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE IF NOT EXISTS test_drop_db.test_table_object_storage1 (n Int32)
    ENGINE = MergeTree
    ORDER BY n
    SETTINGS storage_policy = 'object_storage';

    INSERT INTO test_drop_db.test_table_object_storage1 (n) SELECT number FROM system.numbers LIMIT 10;

    CREATE TABLE IF NOT EXISTS test_drop_db.test_table_object_storage2 (n Int32)
    ENGINE = MergeTree
    ORDER BY n
    SETTINGS storage_policy = 'object_storage';

    INSERT INTO test_drop_db.test_table_object_storage2 (n) SELECT number FROM system.numbers LIMIT 10;
    """
    Then S3 contains greater than 0 objects

    Then save uuid table test_table_object_storage1 in context on clickhouse01
    Then save uuid table test_table_object_storage2 in context on clickhouse01

    Then check s3 disk contains table test_table_object_storage1 data in clickhouse01
    Then check s3 disk contains table test_table_object_storage2 data in clickhouse01

    When we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/data/test_drop_db/
    """
    Then we get response
    """
    test_table_object_storage1
    test_table_object_storage2
    """
    When we execute command on clickhouse01
    """
    chadmin table delete -d test_drop_db -t test_table_%
    """
    Then we get response contains
    """
    """

    Then check table test_table_object_storage1 not exists on s3 disk in clickhouse01
    Then check table test_table_object_storage2 not exists on s3 disk in clickhouse01

    When we execute command on clickhouse01
    """
    ls -l /var/lib/clickhouse/data/test_drop_db/
    """
    Then we get response
    """
    total 0
    """
    Then S3 contains 0 objects

  @require_version_23.3
  Scenario: Drop detached table from local disk
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE IF NOT EXISTS test_drop_db.test_table_local (n Int32)
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO test_drop_db.test_table_local (n) SELECT number FROM system.numbers LIMIT 10;
    """
    Then save uuid table test_table_local in context on clickhouse01
    Then check local disk contains table test_table_local data in clickhouse01
    When we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/data/test_drop_db/
    """
    Then we get response
    """
    test_table_local
    """
    When we execute queries on clickhouse01
    """
    DETACH TABLE test_drop_db.test_table_local SYNC;
    """
    When we execute command on clickhouse01
    """
    chadmin table delete --detached test_drop_db test_table_local
    """
    Then we get response contains
    """
    """
    Then check table test_table_local not exists on local disk in clickhouse01
    When we execute command on clickhouse01
    """
    ls -l /var/lib/clickhouse/data/test_drop_db/
    """
    Then we get response
    """
    total 0
    """

  @require_version_23.3
  Scenario: Drop only one detached table from two detached tables
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE IF NOT EXISTS test_drop_db.test_table_local_deleted (n Int32)
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO test_drop_db.test_table_local_deleted (n) SELECT number FROM system.numbers LIMIT 10;

    CREATE TABLE IF NOT EXISTS test_drop_db.test_table_local_expected (n Int32)
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO test_drop_db.test_table_local_expected (n) SELECT number FROM system.numbers LIMIT 10;
    """

    Then save uuid table test_table_local_deleted in context on clickhouse01
    Then save uuid table test_table_local_expected in context on clickhouse01
    Then check local disk contains table test_table_local_deleted data in clickhouse01
    Then check local disk contains table test_table_local_expected data in clickhouse01

    When we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/data/test_drop_db/
    """
    Then we get response
    """
    test_table_local_deleted
    test_table_local_expected
    """
    When we execute queries on clickhouse01
    """
    DETACH TABLE test_drop_db.test_table_local_deleted SYNC;
    DETACH TABLE test_drop_db.test_table_local_expected SYNC;
    """
    When we execute command on clickhouse01
    """
    chadmin table delete --detached test_drop_db test_table_local_deleted
    """
    Then we get response contains
    """
    """

    Then check table test_table_local_deleted not exists on local disk in clickhouse01
    Then check local disk contains table test_table_local_expected data in clickhouse01

    When we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/data/test_drop_db/
    """
    Then we get response
    """
    test_table_local_expected
    """

  @require_version_23.3
  Scenario: Drop permanently detached table
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE IF NOT EXISTS test_drop_db.test_table_local (n Int32)
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO test_drop_db.test_table_local (n) SELECT number FROM system.numbers LIMIT 10;
    """
    Then save uuid table test_table_local in context on clickhouse01
    Then check local disk contains table test_table_local data in clickhouse01
    When we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/data/test_drop_db/
    """
    Then we get response
    """
    test_table_local
    """
    When we execute queries on clickhouse01
    """
    DETACH TABLE test_drop_db.test_table_local PERMANENTLY SYNC;
    """
    When we execute command on clickhouse01
    """
    chadmin table delete --detached test_drop_db test_table_local
    """
    Then we get response contains
    """
    """
    Then check table test_table_local not exists on local disk in clickhouse01
    When we execute command on clickhouse01
    """
    ls -l /var/lib/clickhouse/data/test_drop_db/
    """
    Then we get response
    """
    total 0
    """

  @require_version_23.3
  Scenario: Drop detached table from object_storage
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE IF NOT EXISTS test_drop_db.test_table_object_storage (n Int32)
    ENGINE = MergeTree
    ORDER BY n
    SETTINGS storage_policy = 'object_storage';

    INSERT INTO test_drop_db.test_table_object_storage (n) SELECT number FROM system.numbers LIMIT 10;
    """
    Then S3 contains greater than 0 objects
    Then save uuid table test_table_object_storage in context on clickhouse01
    Then check s3 disk contains table test_table_object_storage data in clickhouse01

    When we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/data/test_drop_db/
    """
    Then we get response
    """
    test_table_object_storage
    """
    When we execute queries on clickhouse01
    """
    DETACH TABLE test_drop_db.test_table_object_storage SYNC;
    """
    When we execute command on clickhouse01
    """
    chadmin table delete --detached test_drop_db test_table_object_storage
    """
    Then we get response contains
    """
    """
    Then check table test_table_object_storage not exists on s3 disk in clickhouse01
    When we execute command on clickhouse01
    """
    ls -l /var/lib/clickhouse/data/test_drop_db/
    """
    Then we get response
    """
    total 0
    """
    Then S3 contains 0 objects

  @require_version_23.3
  Scenario: Drop replicated table from single host
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE test_drop_db.test_repl
    (n Int32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_repl', '{replica}')
    ORDER BY n;

    INSERT INTO test_drop_db.test_repl (n) SELECT number FROM system.numbers LIMIT 10;
    """
    Then save uuid table test_repl in context on clickhouse01
    Then check local disk contains table test_repl data in clickhouse01

    When we execute queries on clickhouse01
    """
    DETACH TABLE test_drop_db.test_repl SYNC;
    """
    Then the list of children on clickhouse01 for zk node /clickhouse/tables/{shard}/ is equal to
    """
    /clickhouse/tables/shard1/test_repl
    """
    When we execute command on clickhouse01
    """
    chadmin table delete --detached test_drop_db test_repl
    """
    Then we get response contains
    """
    """
    Then check table test_repl not exists on local disk in clickhouse01
    When we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/data/test_drop_db/
    """
    Then we get response
    """
    """
    Then the list of children on clickhouse01 for zk node /clickhouse/tables/{shard}/ is equal to
    """
    """

  @require_version_23.3
  Scenario: Drop replicated table from some hosts
    Given we have executed queries on clickhouse02
    """
    CREATE DATABASE IF NOT EXISTS test_drop_db ON CLUSTER '{cluster}';
    CREATE TABLE test_drop_db.test_repl_expected ON CLUSTER '{cluster}'
    (n Int32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_repl_expected', '{replica}')
    ORDER BY n;
    INSERT INTO test_drop_db.test_repl_expected (n) SELECT number FROM system.numbers LIMIT 10;
    """

    Given we have executed queries on clickhouse01
    """
    CREATE TABLE test_drop_db.test_repl ON CLUSTER '{cluster}'
    (n Int32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_repl', '{replica}')
    ORDER BY n;

    INSERT INTO test_drop_db.test_repl (n) SELECT number FROM system.numbers LIMIT 10;
    """

    Then save uuid table test_repl in context on clickhouse01
    Then save uuid table test_repl_expected in context on clickhouse01
    Then check local disk contains table test_repl data in clickhouse01
    Then check local disk contains table test_repl data in clickhouse02
    Then check local disk contains table test_repl_expected data in clickhouse01
    Then check local disk contains table test_repl_expected data in clickhouse02

    Then the list of children on clickhouse01 for zk node /clickhouse/tables/{shard}/ is equal to
    """
    /clickhouse/tables/shard1/test_repl
    /clickhouse/tables/shard1/test_repl_expected
    """
    Then the list of children on clickhouse02 for zk node /clickhouse/tables/{shard}/ is equal to
    """
    /clickhouse/tables/shard1/test_repl
    /clickhouse/tables/shard1/test_repl_expected
    """

    When we execute queries on clickhouse01
    """
    DETACH TABLE test_drop_db.test_repl PERMANENTLY SYNC;
    """
    When we execute queries on clickhouse02
    """
    DETACH TABLE test_drop_db.test_repl PERMANENTLY SYNC;
    """

    When we execute command on clickhouse01
    """
    chadmin table delete --detached test_drop_db test_repl
    """
    Then we get response contains
    """
    """

    Then check table test_repl not exists on local disk in clickhouse01
    Then check local disk contains table test_repl_expected data in clickhouse01

    When we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/data/test_drop_db/
    """
    Then we get response
    """
    test_repl_expected
    """
    Then the list of children on clickhouse01 for zk node /clickhouse/tables/{shard}/ is equal to
    """
    /clickhouse/tables/shard1/test_repl
    /clickhouse/tables/shard1/test_repl_expected
    """

    When we execute command on clickhouse02
    """
    chadmin table delete --detached test_drop_db test_repl
    """
    Then we get response contains
    """
    """
    Then check table test_repl not exists on local disk in clickhouse02
    Then check local disk contains table test_repl_expected data in clickhouse02

    When we execute command on clickhouse02
    """
    ls /var/lib/clickhouse/data/test_drop_db/
    """
    Then we get response
    """
    test_repl_expected
    """
    Then the list of children on clickhouse02 for zk node /clickhouse/tables/{shard}/ is equal to
    """
    /clickhouse/tables/shard1/test_repl_expected
    """