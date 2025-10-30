Feature: chadmin object-storage commands

  Background:
    Given default configuration
    Given clickhouse-tools configuration on clickhouse01,clickhouse02
    """
    clickhouse:
        user: "_admin"
        password: ""
    object_storage:
      space_usage:
        database: test
        local_blobs_table_prefix: "local_objects_"
        remote_blobs_table_prefix: "listing_objects_from_"
        orphaned_blobs_table_prefix: "orphaned_objects_"
    """
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test ON CLUSTER '{cluster}';

    CREATE TABLE IF NOT EXISTS test.table_s3_01 UUID '10000000-0000-0000-0000-000000000001' ON CLUSTER '{cluster}' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_s3_01', '{replica}') ORDER BY n PARTITION BY (n%10)
    SETTINGS storage_policy = 'object_storage';

    INSERT INTO test.table_s3_01 (n) SELECT number FROM system.numbers LIMIT 10;
    """
    Then S3 contains greater than 0 objects

  @require_version_23.3
  Scenario: Collect info
    When we execute query on clickhouse01
    """
    ALTER TABLE test.table_s3_01 DETACH PARTITION '0';
    """
    And we put object in S3
    """
      bucket: cloud-storage-test
      path: /data/cluster_id/shard_1/orpaned_object.tsv
      data: '1'
    """
    And we execute command on clickhouse01
    """
    chadmin object-storage collect-info --to-time 0h --traverse-remote
    """
    And we execute command on clickhouse01
    """
    chadmin --format yaml object-storage space-usage
    """
    Then we get response matches
    """
    active: '?([1-9][0-9]*)'?
    unique_frozen: '?(0)'?
    unique_detached: '?([1-9][0-9]*)'?
    orphaned: '?(1)'?
    """

  @require_version_24.3
  Scenario: Collect info with frozen parts
    When we execute query on clickhouse01
    """
    ALTER TABLE test.table_s3_01 FREEZE
    """
    And we execute query on clickhouse01
    """
    ALTER TABLE test.table_s3_01 DETACH PARTITION '0';
    """
    And we put object in S3
    """
      bucket: cloud-storage-test
      path: /data/cluster_id/shard_1/orpaned_object.tsv
      data: '1'
    """
    And we execute command on clickhouse01
    """
    chadmin object-storage collect-info --to-time 0h --traverse-remote
    """
    And we execute command on clickhouse01
    """
    chadmin --format yaml object-storage space-usage
    """
    Then we get response matches
    """
    active: '?([1-9][0-9]*)'?
    unique_frozen: '?([1-9][0-9]*)'?
    unique_detached: '?([1-9][0-9]*)'?
    orphaned: '?(1)'?
    """

  Scenario: Dry-run clean with guard period
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --to-time 0h --dry-run
    """
    Then we get response contains
    """
    - WouldDelete: 0
      TotalSize: 0
    """  

  Scenario: Clean orphaned objects
    When we put object in S3
    """
      bucket: cloud-storage-test
      path: /data/cluster_id/shard_1/orpaned_object.tsv
      data: '1'
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --dry-run --to-time 0h
    """
    Then we get response contains
    """
    - WouldDelete: 1
      TotalSize: 1
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --to-time 0h
    """
    Then we get response contains
    """
    - Deleted: 1
      TotalSize: 1
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
    Given merged clickhouse-tools configuration on clickhouse01,clickhouse02
    """
    object_storage:
      clean:
        verify: False
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
    When we try to execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --to-time 0h --prefix "unrelated_path"
    """
    Then we get response contains
    """
    Path validation failed
    """

  Scenario: Clean many orphaned objects with size limit bytes
    When we put 20 objects in S3
    """
      bucket: cloud-storage-test
      path: /data/cluster_id/shard_1/orpaned_object-{}
      data: '13'
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --dry-run --to-time 0h
    """
    Then we get response contains
    """
    - WouldDelete: 20
      TotalSize: 40
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --to-time 0h --max-size-to-delete-bytes 30
    """
    Then we get response contains
    """
    - Deleted: 15
      TotalSize: 30
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --to-time 0h --dry-run
    """
    Then we get response contains
    """
    - WouldDelete: 5
      TotalSize: 10
    """

  Scenario: Clean many orphaned objects with size limit fraction
    Given merged clickhouse-tools configuration on clickhouse01,clickhouse02
    """
    object_storage:
      clean:
        verify: False
    """
    When we put 20 objects in S3
    """
      bucket: cloud-storage-test
      path: /data_2/orpaned_object-{}
      data: '13'
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --dry-run --to-time 0h --prefix "data_2"
    """
    Then we get response contains
    """
    - WouldDelete: 20
      TotalSize: 40
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --to-time 0h --max-size-to-delete-fraction 0.75 --prefix "data_2"
    """
    Then we get response contains
    """
    - Deleted: 15
      TotalSize: 30
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --to-time 0h --dry-run --prefix "data_2"
    """
    Then we get response contains
    """
    - WouldDelete: 5
      TotalSize: 10
    """

  @require_version_23.3
  Scenario: Sanity check clean many orphaned objects
    Given merged clickhouse-tools configuration on clickhouse01,clickhouse02
    """
    object_storage:
      clean:
        verify_size_error_rate_threshold_fraction: 0.05
    """
    When we put 100 objects in S3
    """
      bucket: cloud-storage-test
      path: /data/cluster_id/shard_1/orpaned_object-{}
      data: '01234567890'
    """
    When we try to execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --dry-run --to-time 0h
    """
    Then we get response contains
    """
    - WouldDelete: 100
      TotalSize: 1100
    """
    When we try to execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --to-time 0h
    """
    Then it fails with response contains
    """
    Size validation failed
    """

  @require_version_24.3
  Scenario: Download of missing backups prevents data from removal
    When we execute command on clickhouse01
    """
    ch-backup backup --name test1 --databases test -f
    """
    And we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/disks/object_storage/shadow
    """
    Then we get response
    """
    test1
    """
    When we execute query on clickhouse01
    """
    DROP TABLE test.table_s3_01 SYNC
    """
    And we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --to-time 0h --dry-run
    """
    Then we get response contains
    """
    - WouldDelete: 0
      TotalSize: 0
    """
    When we execute command on clickhouse01
    """
    rm -r /var/lib/clickhouse/disks/object_storage/shadow
    """
    And we execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --to-time 0h --dry-run --ignore-missing-cloud-storage-backups
    """
    Then we get response matches
    """
    - WouldDelete: [1-9][0-9]*
      TotalSize: [1-9][0-9]*
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
    When we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/disks/object_storage/shadow
    """
    Then we get response
    """
    """

  @require_version_24.3
  Scenario: All missing backups are downloaded
    When we execute command on clickhouse01
    """
    ch-backup backup --name test1 --databases test -f &&
    ch-backup backup --name test2 --databases test -f &&
    ch-backup backup --name test3 --databases test -f
    """
    And we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/disks/object_storage/shadow
    """
    Then we get response
    """
    test1
    test2
    test3
    """
    When we execute command on clickhouse01
    """
    rm -r /var/lib/clickhouse/disks/object_storage/shadow/test3
    """
    And we execute command on clickhouse01
    """
    chadmin object-storage clean --to-time 0h --dry-run
    """
    Then we get response contains
    """
    Downloading cloud storage metadata from 'test3'
    """
    When we execute command on clickhouse01
    """
    rm -r /var/lib/clickhouse/disks/object_storage/shadow
    """
    And we execute command on clickhouse01
    """
    chadmin object-storage clean --to-time 0h --dry-run
    """
    Then we get response contains
    """
    Downloading cloud storage metadata from 'test3'
    Downloading cloud storage metadata from 'test2'
    Downloading cloud storage metadata from 'test1'
    """

  Scenario: Sanity check when no objects in CH
    Given we have executed queries on clickhouse01
    """
    DROP TABLE test.table_s3_01 ON CLUSTER '{cluster}' SYNC;
    """
    When we try to execute command on clickhouse01
    """
    chadmin --format yaml object-storage clean --dry-run --to-time 0h
    """
    Then we get response contains
    """
    - WouldDelete: 0
      TotalSize: 0
    """

  @require_version_23.3
  Scenario Outline: Service tables cleanup - old tables are removed with retention_days = 0
    Given merged clickhouse-tools configuration on clickhouse01,clickhouse02
    """
    object_storage:
      space_usage:
        service_tables_retention_days: 0        
    """
    # First, create some service tables by running collect-info
    When we execute command on clickhouse01
    """
    chadmin object-storage collect-info --to-time 0h --traverse-remote
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM system.tables
    WHERE database = 'test'
    AND (name LIKE '%local_objects_object_storage%'
         OR name LIKE '%orphaned_objects_object_storage%'
         OR name LIKE '%listing_objects_from_object_storage%')
    """
    Then we get response
    """
    3
    """
    # Run <command> - this should clean up old unique tables and create new ones
    When we execute command on clickhouse01
    """
    <command>
    """
    # Should still have 3 service tables (cleanup old, create new)
    When we execute query on clickhouse01
    """
    SELECT count() FROM system.tables
    WHERE database = 'test'
    AND (name LIKE '%local_objects_object_storage%'
         OR name LIKE '%orphaned_objects_object_storage%'
         OR name LIKE '%listing_objects_from_object_storage%')
    """
    Then we get response
    """
    3
    """

    Examples:
      | command                                                            |
      | chadmin object-storage collect-info --to-time 0h --traverse-remote |
      | chadmin object-storage clean --to-time 0h --dry-run                |

  @require_version_23.3
  Scenario: Service tables cleanup - retention period works correctly
    Given merged clickhouse-tools configuration on clickhouse01,clickhouse02
    """
    object_storage:
      space_usage:
        service_tables_retention_days: 1
    """
    # Create one old table (older than 1 day) and one recent table (newer than 1 day)
    When we execute query on clickhouse01
    """
    CREATE TABLE test.local_objects_object_storage_old_11111111_1111_1111_1111_111111111111_20240101_120000 (
        obj_path String,
        state String,
        obj_size UInt64,
        ref_count UInt32
    ) ENGINE MergeTree ORDER BY obj_path;
    """
    And we execute query on clickhouse01
    """
    CREATE TABLE test.local_objects_object_storage_new_22222222_2222_2222_2222_222222222222_20991231_120000 (
        obj_path String,
        state String,
        obj_size UInt64,
        ref_count UInt32
    ) ENGINE MergeTree ORDER BY obj_path
    """
    # Run collect-info - should remove old table but keep recent one
    When we execute command on clickhouse01
    """
    chadmin object-storage collect-info --to-time 0h  --traverse-remote
    """
    # Verify old table was removed
    When we execute query on clickhouse01
    """
    SELECT count() FROM system.tables
    WHERE database = 'test'
    AND name = 'local_objects_object_storage_old_11111111_1111_1111_1111_111111111111_20240101_120000'
    """
    Then we get response
    """
    0
    """
    # Verify recent table was kept
    When we execute query on clickhouse01
    """
    SELECT count() FROM system.tables
    WHERE database = 'test'
    AND name = 'local_objects_object_storage_new_22222222_2222_2222_2222_222222222222_20991231_120000'
    """
    Then we get response
    """
    1
    """

  @require_version_23.3
  Scenario: Service tables cleanup - only unique tables are removed
    Given merged clickhouse-tools configuration on clickhouse01,clickhouse02
    """
    object_storage:
      space_usage:
        service_tables_retention_days: 0
        local_blobs_table_prefix: "local_blobs_"
    """
    # Create a non-unique service table (simulating base table without UUID/timestamp)
    When we execute query on clickhouse01
    """
    CREATE TABLE test.local_blobs_object_storage (
        obj_path String,
        state String,
        obj_size UInt64,
        ref_count UInt32
    ) ENGINE MergeTree ORDER BY obj_path
    """
    # Run collect-info - should not touch non-unique tables
    When we execute command on clickhouse01
    """
    chadmin object-storage collect-info --to-time 0h --traverse-remote
    """
    # Verify base table still exists
    When we execute query on clickhouse01
    """
    SELECT count() FROM system.tables
    WHERE database = 'test'
    AND name = 'local_blobs_object_storage'
    """
    Then we get response
    """
    1
    """
