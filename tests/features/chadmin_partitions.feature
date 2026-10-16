Feature: chadmin partitions commands.

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And we have executed queries on clickhouse01
    """
      DROP TABLE IF EXISTS test;
      CREATE TABLE test(a int) ENGINE=MergeTree() ORDER BY a PARTITION BY a;
      INSERT INTO test SELECT 1;
      INSERT INTO test SELECT 2;
      INSERT INTO test SELECT 3;
    """

  Scenario Outline: Detach and attach with precalculated json.
    When we execute command on clickhouse01
    """
    clickhouse client --query "<query>" > /tmp/json
    """
    And we execute command on clickhouse01
    """
    chadmin <test_object> detach <args>
    """
    And we execute query on clickhouse01
    """
    SELECT count() FROM system.parts WHERE table='test' AND active=1
    """
    Then we get response
    """
    0
    """

    When we execute command on clickhouse01
    """
    chadmin <test_object> attach <args>
    """
    And we execute query on clickhouse01
    """
    SELECT count() FROM system.parts WHERE table='test' AND active=1
    """
    Then we get response
    """
    3
    """
    Examples:
        | query                                                                                   | test_object | args                                      |
        | SELECT database, table, partition_id FROM system.parts WHERE table='test' FORMAT JSON   | partition   | --use-partition-list-from-json /tmp/json  |
        | SELECT database, table, name FROM system.parts WHERE table='test' FORMAT JSON           | part        | --use-part-list-from-json /tmp/json       |

  Scenario: Attach partitions detached on the specified disk.
    When we execute queries on clickhouse01
    """
    CREATE TABLE hybrid(a int) ENGINE=MergeTree() ORDER BY a PARTITION BY a SETTINGS storage_policy='hybrid_storage';
    INSERT INTO hybrid SELECT 1;
    INSERT INTO hybrid SELECT 2;
    ALTER TABLE hybrid MOVE PARTITION 2 TO DISK 'object_storage';
    """
    And we execute command on clickhouse01
    """
    chadmin partition detach -t hybrid
    """
    And we execute command on clickhouse01
    """
    chadmin partition attach -t hybrid --disk object_storage
    """
    And we execute query on clickhouse01
    """
    SELECT partition_id FROM system.parts WHERE table='hybrid' AND active=1
    """
    Then we get response
    """
    2
    """
    When we execute query on clickhouse01
    """
    SELECT partition_id FROM system.detached_parts WHERE table='hybrid'
    """
    Then we get response
    """
    1
    """

    When we execute command on clickhouse01
    """
    chadmin partition attach -t hybrid --disk default
    """
    And we execute query on clickhouse01
    """
    SELECT count() FROM system.detached_parts WHERE table='hybrid'
    """
    Then we get response
    """
    0
    """

  Scenario: Reattach with precalculated json.
    When we execute command on clickhouse01
    """
    clickhouse client --query "SELECT database, table, partition_id FROM system.parts WHERE table='test' FORMAT JSON" > /tmp/json
    """
    And we execute command on clickhouse01
    """
    chadmin partition reattach --use-partition-list-from-json /tmp/json
    """
    And we execute query on clickhouse01
    """
    SELECT count() FROM system.parts WHERE table='test' AND active=1
    """
    Then we get response
    """
    3
    """

  Scenario: Reattach a partition that exists only in the replication queue.
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE replication_queue_reattach ON CLUSTER 'cluster' (a Int32)
    ENGINE = ReplicatedMergeTree('/tables/replication_queue_reattach', '{replica}')
    ORDER BY a PARTITION BY a;
    SYSTEM STOP FETCHES replication_queue_reattach;
    """
    And we have executed query on clickhouse02
    """
    INSERT INTO replication_queue_reattach VALUES (42)
    """
    And we have executed query on clickhouse01
    """
    SYSTEM SYNC REPLICA replication_queue_reattach PULL
    """
    And we have executed queries on clickhouse01
    """
    SYSTEM STOP REPLICATION QUEUES replication_queue_reattach;
    SYSTEM START FETCHES replication_queue_reattach;
    """
    And we have executed command on clickhouse01
    """
    clickhouse client --query "ALTER TABLE replication_queue_reattach FETCH PARTITION 42 FROM '/tables/replication_queue_reattach'"
    """
    When we stop clickhouse on clickhouse02
    And we execute query on clickhouse01
    """
    SYSTEM START REPLICATION QUEUES replication_queue_reattach;
    """
    When we execute query on clickhouse01
    """
    SELECT
        (SELECT count() FROM system.parts WHERE table = 'replication_queue_reattach'),
        (SELECT count() > 0 FROM system.detached_parts WHERE table = 'replication_queue_reattach'),
        (SELECT count() > 0 FROM system.replication_queue
         WHERE table = 'replication_queue_reattach'
           AND splitByChar('_', new_part_name)[1] = '42')
    """
    Then we get query response
    """
    0\t1\t1
    """
    When we execute command on clickhouse01
    """
    chadmin partition reattach -t replication_queue_reattach --has-replication-tasks
    """
    Then we get response contains
    """
    ALTER TABLE `default`.`replication_queue_reattach` DETACH PARTITION ID '42'
    """
    And we get response contains
    """
    ALTER TABLE `default`.`replication_queue_reattach` ATTACH PARTITION ID '42'
    """
    When we execute query on clickhouse01
    """
    SELECT
        (SELECT count() FROM system.parts
         WHERE table = 'replication_queue_reattach' AND active),
        (SELECT count() FROM system.detached_parts
         WHERE table = 'replication_queue_reattach'),
        (SELECT count() FROM system.replication_queue
         WHERE table = 'replication_queue_reattach'
           AND splitByChar('_', new_part_name)[1] = '42')
    """
    Then we get query response
    """
    1\t0\t0
    """

  Scenario: Delete with precalculated json.
    When we execute command on clickhouse01
    """
    clickhouse client --query "SELECT database, table, name FROM system.parts WHERE table='test' FORMAT JSON" > /tmp/json
    """
    And we execute command on clickhouse01
    """
    chadmin part detach --use-part-list-from-json /tmp/json
    """
    And we execute command on clickhouse01
    """
    clickhouse client --query "SELECT database, table, name FROM system.detached_parts WHERE table='test' FORMAT JSON" > /tmp/json_detached
    """
    And we execute command on clickhouse01
    """
    chadmin part delete --detached --use-part-list-from-json /tmp/json_detached
    """
    And we execute query on clickhouse01
    """
    SELECT count() FROM system.detached_parts WHERE table='test'
    """
    Then we get response
    """
    0
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM system.parts WHERE table='test' and active=1
    """
    Then we get response
    """
    0
    """

  @require_version_23.1
  Scenario: Drop detached parts.
    When we execute command on clickhouse01
    """
    chadmin part detach -t test
    """
    And we execute command on clickhouse01
    """
    mkdir /var/lib/clickhouse/data/default/test/detached/1_1_1_0_try1
    """
    And we execute command on clickhouse01
    """
    chadmin part delete -t test --detached
    """
    Then we get response contains
    """
    detached/1_1_1_0_try1
    """
    When we execute command on clickhouse01
    """
    clickhouse client --query "SELECT count() FROM system.detached_parts WHERE table='test'"
    """
    Then we get response
    """
    0
    """


  Scenario: Move partitions.
    When we execute queries on clickhouse01
    """
    CREATE DATABASE db;
    CREATE TABLE db.src1 (a UInt32) ENGINE=MergeTree() ORDER by a PARTITION BY a;
    CREATE TABLE db.src2 (a UInt32) ENGINE=MergeTree() ORDER by a;
    CREATE TABLE db.dst1 (a UInt32) ENGINE=MergeTree() ORDER by a PARTITION BY a;
    CREATE TABLE db.dst2 (a UInt32) ENGINE=MergeTree() ORDER by a;


    INSERT INTO db.src1 SELECT number FROM numbers(5);
    INSERT INTO db.src2 SELECT number FROM numbers(5);
    """
    When we execute command on clickhouse01
    """
    chadmin partition move -D db -T dst1 -d db -t src1
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM db.dst1
    """
    Then we get response
    """
    5
    """
    When we execute command on clickhouse01
    """
    chadmin partition move -D db -T dst2 -d db -t src2
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM db.dst2
    """
    Then we get response
    """
    5
    """

  @require_version_23.3
  Scenario: Attach a quarantined S3 partition after reading every object.
    When we execute queries on clickhouse01
    """
    CREATE TABLE checked_s3_attach(id UInt64, payload String)
    ENGINE=MergeTree()
    PARTITION BY tuple()
    ORDER BY id
    SETTINGS storage_policy='object_storage', min_bytes_for_wide_part=0;
    INSERT INTO checked_s3_attach SELECT number, toString(number) FROM numbers(10);
    """
    And we move parts as broken_on_start for table default.checked_s3_attach on clickhouse01
    And we execute queries on clickhouse01
    """
    INSERT INTO checked_s3_attach SELECT number + 10, toString(number) FROM numbers(10);
    ALTER TABLE checked_s3_attach DETACH PARTITION tuple();
    """
    When we execute query on clickhouse01
    """
    SELECT
        countIf(reason = 'broken-on-start'),
        countIf(reason = '')
    FROM system.detached_parts
    WHERE database = 'default' AND table = 'checked_s3_attach'
    """
    Then we get response
    """
    1	1
    """
    When we execute command on clickhouse01
    """
    chadmin partition attach -t checked_s3_attach --reason broken-on-start --check-s3-objects get
    """
    Then we get response contains
    """
    Attach partition default.checked_s3_attach: all
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM checked_s3_attach
    """
    Then we get response
    """
    20
    """

  @require_version_23.3
  Scenario: Leave a quarantined S3 part prefixed when an object is missing.
    When we execute queries on clickhouse01
    """
    CREATE TABLE broken_s3_attach(id UInt64, payload String)
    ENGINE=MergeTree()
    PARTITION BY tuple()
    ORDER BY id
    SETTINGS storage_policy='object_storage', min_bytes_for_wide_part=0;
    INSERT INTO broken_s3_attach VALUES (1, 'payload');
    """
    And we move parts as broken_on_start for table default.broken_s3_attach on clickhouse01
    And we remove S3 blobs for file payload.bin from detached part default.broken_s3_attach on clickhouse01
    And we execute command on clickhouse01
    """
    chadmin partition attach -t broken_s3_attach --reason broken-on-start --check-s3-objects head
    """
    When we execute query on clickhouse01
    """
    SELECT
        (SELECT count() FROM system.parts WHERE table='broken_s3_attach' AND active),
        (SELECT count() FROM system.detached_parts WHERE table='broken_s3_attach' AND reason='broken-on-start')
    """
    Then we get query response
    """
    0\t1
    """

  @require_version_23.3
  Scenario: Quarantine a failed unprefixed S3 part and attach its partition.
    When we execute queries on clickhouse01
    """
    CREATE TABLE quarantine_failed_unprefixed(id UInt64)
    ENGINE=MergeTree()
    PARTITION BY tuple()
    ORDER BY id
    SETTINGS storage_policy='object_storage', min_bytes_for_wide_part=0;
    INSERT INTO quarantine_failed_unprefixed VALUES (1);
    """
    And we move parts as broken_on_start for table default.quarantine_failed_unprefixed on clickhouse01
    And we execute command on clickhouse01
    """
    part_path=$(clickhouse client --query "SELECT path FROM system.detached_parts WHERE table='quarantine_failed_unprefixed' AND reason='broken-on-start'")
    detached_dir=$(dirname "$part_path")
    mkdir "$detached_dir/all_1000000_1000000_0"
    touch "$detached_dir/all_1000000_1000000_0/invalid"
    """
    And we execute command on clickhouse01
    """
    chadmin partition attach -t quarantine_failed_unprefixed --reason broken-on-start --check-s3-objects head --quarantine-failed-unprefixed broken
    """
    When we execute query on clickhouse01
    """
    SELECT
        (SELECT count() FROM system.parts WHERE table='quarantine_failed_unprefixed' AND active),
        (SELECT count() FROM system.detached_parts WHERE table='quarantine_failed_unprefixed' AND reason='broken'),
        (SELECT count() FROM system.detached_parts WHERE table='quarantine_failed_unprefixed' AND reason='broken-on-start')
    """
    Then we get query response
    """
    1\t1\t0
    """

  @require_version_23.3
  Scenario: Remove old zero-copy locks after attaching a quarantined partition.
    When we execute queries on clickhouse01
    """
    DROP TABLE IF EXISTS zero_copy_cleanup ON CLUSTER 'cluster' SYNC;
    CREATE TABLE zero_copy_cleanup UUID '31000000-0000-0000-0000-000000000001' ON CLUSTER 'cluster' (id UInt64)
    ENGINE=ReplicatedMergeTree('/tables/zero_copy_cleanup', '{replica}')
    PARTITION BY tuple()
    ORDER BY id
    SETTINGS storage_policy='object_storage', allow_remote_fs_zero_copy_replication=1;
    INSERT INTO zero_copy_cleanup VALUES (1), (2), (3);
    """
    And we execute command on clickhouse01
    """
    chadmin wait replication-sync --total-timeout 10 --replica-timeout 3
    clickhouse client --query "SELECT name FROM system.parts WHERE table='zero_copy_cleanup' AND active" > /tmp/old-zero-copy-part
    old_part=$(cat /tmp/old-zero-copy-part)
    chadmin zookeeper list "/clickhouse/zero_copy/zero_copy_s3/31000000-0000-0000-0000-000000000001/$old_part" > /tmp/old-zero-copy-unique-id
    """
    When we stop clickhouse on clickhouse02
    And we stop clickhouse on clickhouse01
    And we sleep for 5 seconds
    And we execute command on clickhouse01
    """
    chadmin data-store detach-object-storage-parts
    """
    And we start clickhouse on clickhouse01
    And we execute command on clickhouse01
    """
    chadmin zookeeper exists "/clickhouse/zero_copy/zero_copy_s3/31000000-0000-0000-0000-000000000001/$(cat /tmp/old-zero-copy-part)"
    chadmin partition attach -t zero_copy_cleanup --reason broken-on-start --check-s3-objects head --cleanup-old-zero-copy-locks
    """
    When we execute command on clickhouse01
    """
    old_part=$(cat /tmp/old-zero-copy-part)
    old_unique_id_path=$(cat /tmp/old-zero-copy-unique-id)
    ! chadmin zookeeper exists "$old_unique_id_path"
    chadmin zookeeper exists "/clickhouse/zero_copy/zero_copy_s3/31000000-0000-0000-0000-000000000001/$old_part"
    new_part=$(clickhouse client --query "SELECT name FROM system.parts WHERE table='zero_copy_cleanup' AND active")
    chadmin zookeeper exists "/clickhouse/zero_copy/zero_copy_s3/31000000-0000-0000-0000-000000000001/$new_part"
    """
    And we execute query on clickhouse01
    """
    SELECT count() FROM zero_copy_cleanup
    """
    Then we get response
    """
    3
    """
