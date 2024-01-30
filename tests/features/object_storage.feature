Feature: chadmin object-storage commands

  Background:
    Given default configuration
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

  Scenario: Dry-run clean on one replica with guard period
    When we execute command on clickhouse01
    """
    chadmin object-storage clean --dry-run
    """
    Then we get response contains
    """
    Would delete 0 objects from bucket [cloud-storage-test]
    """
    When we execute command on clickhouse01
    """
    chadmin object-storage clean --to-time 0h --dry-run
    """
    Then we get response matches
    """
    Would delete [1-9][0-9]* objects from bucket \[cloud-storage-test\]
    """

  Scenario: Dry-run clean on cluster with guard period
    When we execute command on clickhouse01
    """
    chadmin object-storage clean --dry-run --on-cluster
    """
    Then we get response contains
    """
    Would delete 0 objects from bucket [cloud-storage-test]
    """
    When we execute command on clickhouse01
    """
    chadmin object-storage clean --to-time 0h --dry-run --on-cluster
    """
    Then we get response contains
    """
    Would delete 0 objects from bucket [cloud-storage-test]
    """

  Scenario: Clean orphaned objects
    When we put object in S3
    """
      bucket: cloud-storage-test
      path: /data/orpaned_object.tsv
      data: '1'
    """
    When we execute command on clickhouse01
    """
    chadmin object-storage clean --dry-run --to-time 0h --on-cluster
    """
    Then we get response contains
    """
    Would delete 1 objects from bucket [cloud-storage-test]
    """
    When we execute command on clickhouse01
    """
    chadmin object-storage clean --to-time 0h --on-cluster
    """
    Then we get response contains
    """
    Deleted 1 objects from bucket [cloud-storage-test]
    """
    And path does not exist in S3
    """
      bucket: cloud-storage-test
      path: /data/orpaned_object.tsv
    """

  Scenario: Clean many orphaned objects
    When we put 100 objects in S3
    """
      bucket: cloud-storage-test
      path: /data/orpaned_object-{}
      data: '1'
    """
    When we execute command on clickhouse01
    """
    chadmin object-storage clean --dry-run --to-time 0h --on-cluster
    """
    Then we get response contains
    """
    Would delete 100 objects from bucket [cloud-storage-test]
    """
    When we execute command on clickhouse01
    """
    chadmin object-storage clean --to-time 0h --on-cluster
    """
    Then we get response contains
    """
    Deleted 100 objects from bucket [cloud-storage-test]
    """
    When we execute command on clickhouse01
    """
    chadmin object-storage clean --to-time 0h --dry-run --on-cluster
    """
    Then we get response contains
    """
    Would delete 0 objects from bucket [cloud-storage-test]
    """

  Scenario: Clean orphaned objects with prefix
    When we put object in S3
    """
      bucket: cloud-storage-test
      path: /data_1/orpaned_object.tsv
      data: '1'
    """
    When we put object in S3
    """
      bucket: cloud-storage-test
      path: /data_2/orpaned_object.tsv
      data: '1'
    """
    When we execute command on clickhouse01
    """
    chadmin object-storage clean --dry-run --to-time 0h --on-cluster --prefix "data_1"
    """
    Then we get response contains
    """
    Would delete 1 objects from bucket [cloud-storage-test]
    """
    When we execute command on clickhouse01
    """
    chadmin object-storage clean --to-time 0h --on-cluster --prefix "data_1"
    """
    Then we get response contains
    """
    Deleted 1 objects from bucket [cloud-storage-test]
    """
    And path does not exist in S3
    """
      bucket: cloud-storage-test
      path: /data_1/orpaned_object.tsv
    """
    When we execute command on clickhouse01
    """
    chadmin object-storage clean --dry-run --to-time 0h --on-cluster --prefix "data_2"
    """
    Then we get response contains
    """
    Would delete 1 objects from bucket [cloud-storage-test]
    """