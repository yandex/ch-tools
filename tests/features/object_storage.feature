Feature: chadmin object-storage commands

  Background:
    Given default configuration
    Given clickhouse-tools configuration on clickhouse01,clickhouse02
    """
    clickhouse:
        user: "_admin"
        password: ""
    """
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test ON CLUSTER '{cluster}';

    CREATE TABLE IF NOT EXISTS test.table_s3_01 ON CLUSTER '{cluster}' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_s3_01', '{replica}') ORDER BY n
    SETTINGS storage_policy = 'object_storage';

    INSERT INTO test.table_s3_01 (n) SELECT number FROM system.numbers LIMIT 10;
    """
    Then S3 contains greater than 0 objects

  Scenario Outline: Dry-run clean with guard period
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --to-time 0h --dry-run --clean-scope=<scope>
    """
    Then we get response matches
    """
    - WouldDelete: <WouldDelete>
      TotalSize: <TotalSize>
    """  
  Examples:
    | scope   | WouldDelete | TotalSize   |
    | host    | [1-9][0-9]* | [1-9][0-9]* |
    | shard   | 0           | 0           |
    | cluster | 0           | 0           |

  Scenario Outline: Clean orphaned objects
    When we put object in S3
    """
      bucket: cloud-storage-test
      path: /data/cluster_id/shard_1/orpaned_object.tsv
      data: '1'
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --dry-run --to-time 0h --clean-scope=<scope>
    """
    Then we get response contains
    """
    - WouldDelete: <WouldDelete>
      TotalSize: <TotalSize>
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --to-time 0h --clean-scope=<scope>
    """
    Then we get response contains
    """
    - Deleted: <Deleted>
      TotalSize: <TotalSize>
    """
    And path does not exist in S3
    """
      bucket: cloud-storage-test
      path: /data/orpaned_object.tsv
    """
  Examples:
    | scope   | WouldDelete | TotalSize   | Deleted |
    | shard   | 1           | 1           | 1       |
    | cluster | 1           | 1           | 1       |
  
  Scenario: Clean many orphaned objects
    When we put 100 objects in S3
    """
      bucket: cloud-storage-test
      path: /data/cluster_id/shard_1/orpaned_object-{}
      data: '10'
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --dry-run --to-time 0h
    """
    Then we get response contains
    """
    - WouldDelete: 100
      TotalSize: 200
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --to-time 0h
    """
    Then we get response contains
    """
    - Deleted: 100
      TotalSize: 200
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --to-time 0h --dry-run
    """
    Then we get response contains
    """
    - WouldDelete: 0
      TotalSize: 0
    """
 
  Scenario: Clean orphaned objects with prefix
    Given clickhouse-tools configuration on clickhouse01,clickhouse02
    """
    object_storage:
      clean:
        perform_sanity_check_size: False
        perform_sanity_check_paths: False
    """
    When we put object in S3
    """
      bucket: cloud-storage-test
      path: /data_1/orpaned_object.tsv
      data: '10'
    """
    When we put object in S3
    """
      bucket: cloud-storage-test
      path: /data_2/orpaned_object.tsv
      data: '100'
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --dry-run --to-time 0h --prefix "data_1"
    """
    Then we get response contains
    """
    - WouldDelete: 1
      TotalSize: 2
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --to-time 0h --prefix "data_1"
    """
    Then we get response contains
    """
    - Deleted: 1
      TotalSize: 2
    """
    And path does not exist in S3
    """
      bucket: cloud-storage-test
      path: /data_1/orpaned_object.tsv
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --dry-run --to-time 0h --prefix "data_2"
    """
    Then we get response contains
    """
    - WouldDelete: 1
      TotalSize: 3
    """
  
  Scenario: Clean with store-state-zk-path works
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --dry-run --to-time 0h --keep-paths --store-state-zk-path /tmp/shard_1
    """
    Then we get zookeeper node with "/tmp/shard_1" path
    """
    {
        "orphaned_objects_size": 0,
        "error_msg": ""
    }
    """
    When we put object in S3
    """
      bucket: cloud-storage-test
      path: /data/orpaned_object.tsv
      data: '1234567890'
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --dry-run --to-time 0h --keep-paths --store-state-zk-path /tmp/shard_1
    """
    Then we get zookeeper node with "/tmp/shard_1" path
    """
    {
        "orphaned_objects_size": 10,
        "error_msg": ""
    }
    """

  Scenario: Clean with store-state-local works
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --dry-run --to-time 0h --keep-paths --store-state-local
    """
    Then we get file /tmp/object_storage_cleanup_state.json
    """
    {
        "orphaned_objects_size": 0,
        "error_msg": ""
    }
    """
    When we put object in S3
    """
      bucket: cloud-storage-test
      path: /data/cluster_id/shard_1/orpaned_object.tsv
      data: '1234567890'
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --dry-run --to-time 0h --keep-paths --store-state-local
    """
    Then we get file /tmp/object_storage_cleanup_state.json
    """
    {
        "orphaned_objects_size": 10,
        "error_msg": ""
    }
    """

  Scenario: Check incorrect file path
    When we put object in S3
    """
      bucket: cloud-storage-test
      path: /unrelated_path/orpaned_object.tsv
      data: '100'
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --dry-run --to-time 0h --prefix "unrelated_path"
    """
    Then we get response contains
    """
    Sanity check not passed
    """
