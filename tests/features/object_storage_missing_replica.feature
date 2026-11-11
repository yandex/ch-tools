@with_missing_replica
Feature: Object storage with a configured replica that does not exist

  Background:
    Given a working S3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And clickhouse-tools configuration on clickhouse01
    """
    clickhouse:
      user: _admin
      retries:
        max_attempts: 1
    """

  @require_version_23.3
  Scenario: Collect, report and clean objects with a missing replica
    When we execute queries on clickhouse02
    """
    CREATE TABLE live_objects (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS storage_policy = 'object_storage';
    INSERT INTO live_objects VALUES (42);
    """
    And we execute queries on clickhouse01
    """
    CREATE TABLE listing_objects_from_object_storage_11111111_1111_1111_1111_111111111111_20240101_120000 (n UInt8) ENGINE = Memory;
    """
    And we put object in S3
    """
    bucket: cloud-storage-test
    path: data/cluster_id/shard_1/orphan
    data: 'abc'
    """
    And we try to execute command on clickhouse01
    """
    chadmin object-storage collect-info --to-time 0h --traverse-remote
    """
    Then it fails with response contains
    """
    Unavailable replicas without an exemption: clickhouse03.invalid
    """
    Given merged clickhouse-tools configuration on clickhouse01
    """
    object_storage:
      unavailable_hosts: [clickhouse03.invalid]
    """
    When we execute command on clickhouse01
    """
    chadmin object-storage collect-info --to-time 0h --traverse-remote
    """
    And we execute command on clickhouse01
    """
    chadmin --format yaml object-storage space-usage
    """
    Then we get response matches
    """
    total:
      active: '?[1-9][0-9]*'?
      orphaned: '?3'?$
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage space-usage --scope shard
    """
    Then we get response matches
    """
    total:
      active: '?[1-9][0-9]*'?
      orphaned: '?3'?$
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --to-time 0h --dry-run --use-saved-list --keep-paths
    """
    Then we get response contains
    """
      WouldDeleted: 1
      WouldDeletedSize: 3
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --to-time 0h
    """
    Then we get response contains
    """
      Deleted: 1
      DeletedSize: 3
    """
    And path does not exist in S3
    """
    bucket: cloud-storage-test
    path: data/cluster_id/shard_1/orphan
    """
    When we execute query on clickhouse02
    """
    SELECT n FROM live_objects
    """
    Then we get response
    """
    42
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM system.tables
    WHERE database = 'default' AND startsWith(name, 'listing_objects_from_object_storage_')
    """
    Then we get response
    """
    0
    """

  @require_version_23.3
  Scenario: Cluster space usage requires one response from each shard
    When we execute queries on clickhouse01
    """
    CREATE TABLE space_usage_from_object_storage (replica Array(String), state String, size UInt64) ENGINE = Memory;
    INSERT INTO space_usage_from_object_storage VALUES (['clickhouse01'], 'active', 100), (['unknown_replicas'], 'orphaned', 3);
    """
    And we execute queries on clickhouse02
    """
    CREATE TABLE space_usage_from_object_storage (replica Array(String), state String, size UInt64) ENGINE = Memory;
    """
    And we try to execute command on clickhouse01
    """
    chadmin object-storage space-usage --cluster two_shards
    """
    Then it fails with response contains
    """
    Unavailable replicas without an exemption: clickhouse03.invalid
    """
    Given merged clickhouse-tools configuration on clickhouse01
    """
    object_storage:
      unavailable_hosts: [clickhouse03.invalid]
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage space-usage --cluster two_shards
    """
    Then we get response contains
    """
    total:
      active: 100
      orphaned: 3
    """
    When we execute queries on clickhouse02
    """
    INSERT INTO space_usage_from_object_storage VALUES (['clickhouse02'], 'active', 200), (['unknown_replicas'], 'orphaned', 7);
    """
    And we execute command on clickhouse01
    """
    chadmin --format yaml object-storage space-usage --cluster two_shards
    """
    Then we get response contains
    """
    total:
      active: 300
      orphaned: 10
    """
    When we try to execute command on clickhouse01
    """
    chadmin object-storage space-usage --cluster missing_shard
    """
    Then it fails with response contains
    """
    No available replicas remain in shard 2 of cluster 'missing_shard'
    """
