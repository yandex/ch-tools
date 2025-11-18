Feature: chadmin zero-copy related zookeeper commands.

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02


  Scenario Outline: Create zero-copy locks for all tables
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS test ON CLUSTER 'cluster'; 
    CREATE DATABASE test ON CLUSTER 'cluster';
    DROP DATABASE IF EXISTS test1 ON CLUSTER 'cluster'; 
    CREATE DATABASE test1 ON CLUSTER 'cluster';

    CREATE TABLE test.table_01 UUID '10000000-0000-0000-0000-000000000001' ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n
    SETTINGS storage_policy='object_storage',allow_remote_fs_zero_copy_replication=1;
    INSERT INTO test.table_01 SELECT number FROM numbers(2);

    CREATE TABLE test1.table_02 UUID '10000000-0000-0000-0000-000000000002' ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_02', '{replica}') PARTITION BY n ORDER BY n
    SETTINGS storage_policy='object_storage',allow_remote_fs_zero_copy_replication=1;
    INSERT INTO test1.table_02 SELECT number FROM numbers(2);
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --table-uuid 10000000-0000-0000-0000-000000000001 && \
    chadmin zookeeper cleanup-zero-copy-locks --table-uuid 10000000-0000-0000-0000-000000000002
    """
    Then the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3 is empty
    When we execute command on clickhouse01
    """
    chadmin zookeeper create-zero-copy-locks --disk object_storage <replicas>
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper list $(chadmin zookeeper list /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_0/)
    """
    Then we get response contains
    """
    <result1>
    """
    Then we get response contains
    """
    <result2>
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper list $(chadmin zookeeper list /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000002/0_0_0_0/)
    """
    Then we get response contains
    """
    <result1>
    """
    Then we get response contains
    """
    <result2>
    """
  Examples:
      | replicas                                                            | result1                     | result2                    |
      | --replicas clickhouse01.ch_tools_test,clickhouse02.ch_tools_test    | clickhouse01.ch_tools_test  | clickhouse02.ch_tools_test |
      | --replicas clickhouse02.ch_tools_test                               | clickhouse02.ch_tools_test  | clickhouse02.ch_tools_test |
      | --all-replicas                                                      | clickhouse01.ch_tools_test  | clickhouse02.ch_tools_test |
      |                                                                     | clickhouse01.ch_tools_test  | clickhouse01.ch_tools_test |


  Scenario: Create zero-copy lock options for all tables in database
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS test ON CLUSTER 'cluster'; 
    CREATE DATABASE test ON CLUSTER 'cluster';

    CREATE TABLE test.table_01 UUID '10000000-0000-0000-0000-000000000001' ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n
    SETTINGS storage_policy='object_storage',allow_remote_fs_zero_copy_replication=1;
    INSERT INTO test.table_01 SELECT number FROM numbers(2);

    CREATE TABLE test.table_02 UUID '10000000-0000-0000-0000-000000000002' ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_02', '{replica}') PARTITION BY n ORDER BY n
    SETTINGS storage_policy='object_storage',allow_remote_fs_zero_copy_replication=1;
    INSERT INTO test.table_02 SELECT number FROM numbers(2);

    CREATE TABLE test.table_03 ON CLUSTER 'cluster' (n Int32)
    ENGINE = MergeTree ORDER BY n
    SETTINGS storage_policy='object_storage';
    INSERT INTO test.table_03 SELECT number FROM numbers(2);
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --table-uuid 10000000-0000-0000-0000-000000000001 --part-id 0_0_0_0 --replica clickhouse01.ch_tools_test
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --table-uuid 10000000-0000-0000-0000-000000000002
    """
    Then the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3 is equal to
    """
    /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper create-zero-copy-locks -d test --disk object_storage
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper list $(chadmin zookeeper list /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_0/)
    """
    Then we get response contains
    """
    /clickhouse01.ch_tools_test
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper list $(chadmin zookeeper list /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/1_0_0_0/)
    """
    Then we get response contains
    """
    /clickhouse01.ch_tools_test
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper list $(chadmin zookeeper list /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000002/0_0_0_0/)
    """
    Then we get response contains
    """
    /clickhouse01.ch_tools_test
    """

  Scenario Outline: Create zero-copy lock for part or partition
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS test ON CLUSTER 'cluster'; 
    CREATE DATABASE test ON CLUSTER 'cluster';

    CREATE TABLE test.table_01 UUID '10000000-0000-0000-0000-000000000001' ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n
    SETTINGS storage_policy='object_storage',allow_remote_fs_zero_copy_replication=1;
    INSERT INTO test.table_01 SELECT number FROM numbers(2);
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --table-uuid 10000000-0000-0000-0000-000000000001 --replica clickhouse02.ch_tools_test
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper create-zero-copy-locks -t table_01 <option> --replicas clickhouse02.ch_tools_test --disk object_storage
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper list $(chadmin zookeeper list /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_0/)
    """
    Then we get response contains
    """
    /clickhouse02.ch_tools_test
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --table-uuid 10000000-0000-0000-0000-000000000001 --replica clickhouse01.ch_tools_test
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper create-zero-copy-locks -t table_01 <option> --disk object_storage
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper list $(chadmin zookeeper list /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_0/)
    """
    Then we get response contains
    """
    /clickhouse01.ch_tools_test
    """
  Examples:
      | option               |
      | --partition-id 0     |
      | --part-id 0_0_0_0    |


  Scenario: Cleanup all zero-copy locks for two replicas with one lock for third replica present
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS test ON CLUSTER 'cluster'; 
    CREATE DATABASE test ON CLUSTER 'cluster';

    CREATE TABLE test.table_01 UUID '10000000-0000-0000-0000-000000000001' ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n
    SETTINGS storage_policy='object_storage',allow_remote_fs_zero_copy_replication=1;
    INSERT INTO test.table_01 SELECT number FROM numbers(2);

    CREATE TABLE test.table_02 UUID '10000000-0000-0000-0000-000000000002' ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_02', '{replica}') PARTITION BY n ORDER BY n
    SETTINGS storage_policy='object_storage',allow_remote_fs_zero_copy_replication=1;
    INSERT INTO test.table_02 SELECT number FROM numbers(2);
    """
    And we execute command on clickhouse01
    """
    chadmin wait replication-sync --total-timeout 10 --replica-timeout 3
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper create --make-parents /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_0/blob_path/clickhouse03.ch_tools_test
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --replica clickhouse01.ch_tools_test
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --replica clickhouse01.ch_tools_test --dry-run
    """
    Then we get response
    """
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --replica clickhouse02.ch_tools_test --dry-run
    """
    Then we get response contains 
    """
    clickhouse02.ch_tools_test
    """
    And the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_0/blob_path/ is equal to
    """
    /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_0/blob_path/clickhouse03.ch_tools_test
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --replica clickhouse02.ch_tools_test
    """
    Then the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3/ is equal to
    """
    /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001
    """
    And the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001 is equal to
    """
    /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_0
    """


  Scenario: Cleanup all zero-copy locks for replicas with table and part filters
    When we execute command on clickhouse01
    """
    chadmin zookeeper create --make-parents /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_1/blob1/replica1
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper create --make-parents /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_1/blob1/replica2
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper create --make-parents /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_1/blob2/replica1
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper create --make-parents /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_2/blob1/replica1
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper create --make-parents /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000002/0_0_0_1/blob1/replica1
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --table-uuid 10000000-0000-0000-0000-000000000002 --replica replica1
    """
    Then the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3/ is equal to
    """
    /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --table-uuid 10000000-0000-0000-0000-000000000001 --part-id 0_0_0_2 --replica replica1
    """
    Then the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001 is equal to
    """
    /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_1
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --table-uuid 10000000-0000-0000-0000-000000000001 --part-id 0_0_0_1 --replica replica1
    """
    Then the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_1/blob1 is equal to
    """
    /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_1/blob1/replica2
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --table-uuid 10000000-0000-0000-0000-000000000001 --replica replica2
    """
    Then the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3 is empty


  Scenario: Cleanup zero-copy locks without replica filter
    When we execute command on clickhouse01
    """
    chadmin zookeeper create --make-parents /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_1/blob1/replica1
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper create --make-parents /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_1/blob1/replica2
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper create --make-parents /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_1/blob2/replica1
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper create --make-parents /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/all_0_0_2_1/blob1/replica1
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper create --make-parents /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000002/0_0_0_1/blob1/replica1
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --table-uuid 10000000-0000-0000-0000-000000000002
    """
    Then the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3/ is equal to
    """
    /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --table-uuid 10000000-0000-0000-0000-000000000001 --part-id all_0_0_2_1
    """
    Then the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001 is equal to
    """
    /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_1
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --table-uuid 10000000-0000-0000-0000-000000000001 --part-id 0_0_0_1
    """
    Then the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3 is empty

  Scenario: Cleanup all zero-copy locks for table with per-table zookeeper zero-copy path
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS test ON CLUSTER 'cluster'; 
    CREATE DATABASE test ON CLUSTER 'cluster';

    CREATE TABLE test.table_01 UUID '10000000-0000-0000-0000-000000000001' ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n
    SETTINGS storage_policy='object_storage',allow_remote_fs_zero_copy_replication=1,remote_fs_zero_copy_zookeeper_path='/custom/zero_copy/';
    INSERT INTO test.table_01 SELECT number FROM numbers(2);
    """
    Then the list of children on clickhouse01 for zk node /custom/zero_copy/zero_copy_s3 is equal to
    """
    /custom/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --table-uuid 10000000-0000-0000-0000-000000000001
    """
    Then the list of children on clickhouse01 for zk node /custom/zero_copy/zero_copy_s3 is empty


  Scenario: Cleanup all zero-copy locks for given remote path prefix
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS test ON CLUSTER 'cluster'; 
    CREATE DATABASE test ON CLUSTER 'cluster';

    CREATE TABLE test.table_01 UUID '10000000-0000-0000-0000-000000000001' ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n
    SETTINGS storage_policy='object_storage',allow_remote_fs_zero_copy_replication=1;
    INSERT INTO test.table_01 SELECT number FROM numbers(2);
    """
    Then the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/ is equal to
    """
    /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_0
    /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/1_0_0_0
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --remote-path-prefix 'data_cluster_id'
    """
    Then the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3/ is empty


  Scenario: Cleanup zero-copy locks with invalid arguments
    When we try to execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --table-uuid 10000000-0000-0000-0000-00000asdasd
    """
    Then it fails with response contains
    """
    10000000-0000-0000-0000-00000asdasd
    """
    When we try to execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --table-uuid 10000000-0000-0000-0000-000000000001 --part-id all_0_all_all
    """
    Then it fails with response contains
    """
    all_0_all_all
    """
    When we try to execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --zero-copy-path /clickhouse/tables --replica r1
    """
    Then it fails with response contains
    """
    /clickhouse/tables
    """
    When we try to execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --remote-path-prefix cloud_storage/shard1/
    """
    Then it fails with response contains
    """
    cloud_storage/shard1/
    """


  Scenario: Create zero-copy locks with custom zero-copy path
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS test ON CLUSTER 'cluster';
    CREATE DATABASE test ON CLUSTER 'cluster';

    CREATE TABLE test.table_01 UUID '10000000-0000-0000-0000-000000000001' ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n
    SETTINGS storage_policy='object_storage',allow_remote_fs_zero_copy_replication=1;
    INSERT INTO test.table_01 SELECT number FROM numbers(2);
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --table-uuid 10000000-0000-0000-0000-000000000001
    """
    Then the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3 is empty
    When we execute command on clickhouse01
    """
    chadmin zookeeper create-zero-copy-locks --disk object_storage --zero-copy-path /custom/zero_copy/zero_copy_s3 -t table_01
    """
    Then the list of children on clickhouse01 for zk node /custom/zero_copy/zero_copy_s3 is equal to
    """
    /custom/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper list $(chadmin zookeeper list /custom/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/0_0_0_0/)
    """
    Then we get response contains
    """
    /clickhouse01.ch_tools_test
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper list $(chadmin zookeeper list /custom/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001/1_0_0_0/)
    """
    Then we get response contains
    """
    /clickhouse01.ch_tools_test
    """
    Then the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3 is empty


  Scenario: Create zero-copy locks for large number of parts
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS test ON CLUSTER 'cluster';
    CREATE DATABASE test ON CLUSTER 'cluster';

    CREATE TABLE test.table_many_parts UUID '20000000-0000-0000-0000-000000000001' ON CLUSTER 'cluster' (
        id Int32,
        partition_key Int32
    )
    ENGINE = ReplicatedMergeTree('/tables/table_many_parts', '{replica}')
    PARTITION BY partition_key
    ORDER BY id
    SETTINGS storage_policy='object_storage',allow_remote_fs_zero_copy_replication=1;
    """
    And we execute queries on clickhouse01
    """
    INSERT INTO test.table_many_parts
    SELECT
        number as id,
        number as partition_key
    FROM numbers(100) SETTINGS max_partitions_per_insert_block=100;
    """
    And we execute command on clickhouse01
    """
    chadmin zookeeper cleanup-zero-copy-locks --table-uuid 20000000-0000-0000-0000-000000000001
    """
    Then the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3 is empty
    When we execute command on clickhouse01
    """
    chadmin zookeeper create-zero-copy-locks --disk object_storage --max-workers 8 --zero-copy-path /custom_large/zero_copy/zero_copy_s3 -t table_many_parts
    """
    Then the list of children on clickhouse01 for zk node /custom_large/zero_copy/zero_copy_s3 is equal to
    """
    /custom_large/zero_copy/zero_copy_s3/20000000-0000-0000-0000-000000000001
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper list /custom_large/zero_copy/zero_copy_s3/20000000-0000-0000-0000-000000000001 | wc -l
    """
    Then we get response contains
    """
    100
    """

