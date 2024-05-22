Feature: ch-monitoring tool

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    # Create test data set.
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test ON CLUSTER 'cluster';

    CREATE TABLE IF NOT EXISTS test.table_01 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n;

    CREATE TABLE IF NOT EXISTS test.dtable_01 ON CLUSTER 'cluster' AS test.table_01
    ENGINE = Distributed('cluster', 'test', 'table_01', n);

    INSERT INTO test.dtable_01 (n) SELECT number FROM system.numbers LIMIT 10;

    CREATE TABLE IF NOT EXISTS test.test_unfreeze (id int, name String) ENGINE=MergeTree() ORDER BY id SETTINGS storage_policy='object_storage';
    INSERT INTO test.test_unfreeze VALUES(5, 'hello');
    """
  
  Scenario: Check CH orphaned objects size
    When we execute command on clickhouse01
    """
    ch-monitoring orphaned-objects
    """
    Then we get response
    """
    0;Total size: 0
    """
    When we put object in S3
    """
      bucket: cloud-storage-test
      path: /data/orpaned_object.tsv
      data: '1'
    """
    When we execute command on clickhouse01
    """
    ch-monitoring orphaned-objects --to-time 0h
    """
    Then we get response contains
    """
    0;Total size: 1
    """
