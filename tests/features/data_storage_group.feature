Feature: chadmin data-store commands

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    
  Scenario: Check collect clean orphaned data.
    When we execute command on clickhouse01
    """
    mkdir -p /var/lib/clickhouse/disks/object_storage/store/123/1234678-f435-49dd-9358-fffc12b758b0
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml data-store clean-orphaned-tables --store-path /var/lib/clickhouse/disks/object_storage/store
    """
    Then we get response contains
    """
    - path: /var/lib/clickhouse/disks/object_storage/store/123
      status: not_used
      size: 8.0K
      removed: false
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml data-store clean-orphaned-tables --store-path /var/lib/clickhouse/disks/object_storage/store --remove
    """
    Then we get response contains
    """
    - path: /var/lib/clickhouse/disks/object_storage/store/123
      status: not_used
      size: 8.0K
      removed: true
    """

  Scenario: Check remove orphaned sql object whole db.
    When we execute command on clickhouse01
    """
    mkdir -p /var/lib/clickhouse/disks/object_storage/data/db2/test2
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml data-store cleanup-data-dir --disk object_storage
    """
    Then we get response contains
    """
    - path: /var/lib/clickhouse/disks/object_storage/data/db2
      deleted: 'No'
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml data-store cleanup-data-dir --disk object_storage --remove
    """
    Then we get response contains
    """
    - path: /var/lib/clickhouse/disks/object_storage/data/db2
      deleted: 'Yes'
    """

  Scenario: Check remove orphaned sql object single table.
    When we execute queries on clickhouse01
    """
    CREATE DATABASE db1 Engine=Lazy(20);
    CREATE TABLE db1.test (a int) Engine=StripeLog() SETTINGS storage_policy='object_storage';
    """
    When we execute command on clickhouse01
    """
    mkdir -p /var/lib/clickhouse/disks/object_storage/data/db1/test1
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml data-store cleanup-data-dir --disk object_storage
    """
    Then we get response contains
    """
    - path: /var/lib/clickhouse/disks/object_storage/data/db1/test1
      deleted: 'No'
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml data-store cleanup-data-dir --disk object_storage --remove
    """
    Then we get response contains
    """
    - path: /var/lib/clickhouse/disks/object_storage/data/db1/test1
      deleted: 'Yes'
    """

  # TODO: enable after detach partition fix
  @skip
  Scenario Outline: Reattach partitions with broken parts from zero copy
    When we execute queries on clickhouse01
    """
    SYSTEM STOP MERGES;

    DROP DATABASE IF EXISTS test_db;
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table1 (a int, b int) ENGINE=ReplicatedMergeTree('/clickhouse/{database}/{table}', '{replica}') ORDER BY a PARTITION BY a 
    SETTINGS storage_policy='object_storage', allow_remote_fs_zero_copy_replication=1 <additional_table_settings>;
    INSERT INTO test_db.table1 SELECT 1, 1;
    INSERT INTO test_db.table1 SELECT 1, 2;
    INSERT INTO test_db.table1 SELECT 2, 1;
    INSERT INTO test_db.table1 SELECT 3, 1;

    CREATE TABLE test_db.table2 (a int, b int) ENGINE=ReplicatedMergeTree('/clickhouse/{database}/{table}', '{replica}') ORDER BY a PARTITION BY a 
    SETTINGS storage_policy='object_storage', allow_remote_fs_zero_copy_replication=1 <additional_table_settings>;

    INSERT INTO test_db.table2 SELECT 1, 1;
    INSERT INTO test_db.table2 SELECT 2, 1;
    INSERT INTO test_db.table2 SELECT 3, 1;
    """
    # Imitate "bad" behavior of zero copy, when s3 objects are removed earlier than needed.
    And we remove key from s3 for partitions database test_db on clickhouse01
    """
    test_db:
      table1: ['1', '2']
      table2: ['1', '2', '3']
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml data-store detect-broken-partitions
    """
    Then we get response contains
    """
    - table: '`test_db`.`table1`'
      partition: '1'
    - table: '`test_db`.`table1`'
      partition: '2'
    - table: '`test_db`.`table2`'
      partition: '1'
    - table: '`test_db`.`table2`'
      partition: '2'
    - table: '`test_db`.`table2`'
      partition: '3'
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml data-store detect-broken-partitions --reattach
    """
    When we execute queries on clickhouse01
    """
    SELECT * FROM test_db.table1;
    SELECT * FROM test_db.table2;
    """

    @require_version_24.4
    Examples:
    | additional_table_settings                               |
    | , disable_detach_partition_for_zero_copy_replication = 0|

    @require_version_less_than_24.4
    Examples:
    | additional_table_settings                               |
    |                                                         |

  # TODO: enable after detach partition fix
  @skip
  Scenario Outline: Detach partitions with broken parts from zero copy
    When we execute queries on clickhouse01
    """
    SYSTEM STOP MERGES;

    DROP DATABASE IF EXISTS test_db;
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table1 (a int, b int) ENGINE=ReplicatedMergeTree('/clickhouse/{database}/{table}', '{replica}') ORDER BY a PARTITION BY a 
    SETTINGS storage_policy='object_storage', allow_remote_fs_zero_copy_replication=1 <additional_table_settings>;
    INSERT INTO test_db.table1 SELECT 1, 1;
    INSERT INTO test_db.table1 SELECT 1, 2;
    INSERT INTO test_db.table1 SELECT 2, 1;
    INSERT INTO test_db.table1 SELECT 3, 1;

    CREATE TABLE test_db.table2 (a int, b int) ENGINE=ReplicatedMergeTree('/clickhouse/{database}/{table}', '{replica}') ORDER BY a PARTITION BY a 
    SETTINGS storage_policy='object_storage', allow_remote_fs_zero_copy_replication=1 <additional_table_settings>;

    INSERT INTO test_db.table2 SELECT 1, 1;
    INSERT INTO test_db.table2 SELECT 2, 1;
    INSERT INTO test_db.table2 SELECT 3, 1;
    """
    # Imitate "bad" behavior of zero copy, when s3 objects are removed earlier than needed.
    And we remove key from s3 for partitions database test_db on clickhouse01
    """
    test_db:
      table1: ['1', '2']
      table2: ['1', '2', '3']
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml data-store detect-broken-partitions
    """
    Then we get response contains
    """
    - table: '`test_db`.`table1`'
      partition: '1'
    - table: '`test_db`.`table1`'
      partition: '2'
    - table: '`test_db`.`table2`'
      partition: '1'
    - table: '`test_db`.`table2`'
      partition: '2'
    - table: '`test_db`.`table2`'
      partition: '3'
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml data-store detect-broken-partitions --detach
    """
    When we execute queries on clickhouse01
    """
    SELECT * FROM test_db.table1;
    SELECT * FROM test_db.table2;
    """

    @require_version_24.4
    Examples:
    | additional_table_settings                               |
    | , disable_detach_partition_for_zero_copy_replication = 0|

    @require_version_less_than_24.4
    Examples:
    | additional_table_settings                               |
    |                                                         |

  @require_version_23.3
  Scenario: Attach broken partitions.
    When we execute queries on clickhouse01
    """
    CREATE DATABASE test;
    CREATE TABLE test.table(a UInt32, b UInt32) ENGINE=MergeTree() ORDER BY a PARTITION BY b;
    INSERT INTO test.table SELECT number, number FROM numbers(3);
    """
    When we move parts as broken_on_start for table test.table on clickhouse01
    And we execute command on clickhouse01
    """
    chadmin part remove-detached-part-prefix --reason broken-on-start --all
    chadmin partition attach --all
    """
    And we execute query on clickhouse01
    """
    SELECT count() FROM test.table
    """
    Then we get response
    """
    3
    """
