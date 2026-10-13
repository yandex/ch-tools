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

  @require_version_less_than_26.1
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

  Scenario: Offline-detach object-storage parts while the second replica is unavailable.
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS quarantine_parts ON CLUSTER 'cluster' SYNC;
    CREATE DATABASE quarantine_parts ON CLUSTER 'cluster';
    CREATE TABLE quarantine_parts.parts UUID '20000000-0000-0000-0000-000000000001' ON CLUSTER 'cluster' (n UInt32)
    ENGINE = ReplicatedMergeTree('/tables/quarantine_parts', '{replica}')
    PARTITION BY n ORDER BY n SETTINGS storage_policy = 'object_storage';
    INSERT INTO quarantine_parts.parts VALUES (0);
    """
    And we execute command on clickhouse01
    """
    chadmin wait replication-sync --total-timeout 10 --replica-timeout 3
    """
    And we execute query on clickhouse02
    """
    SELECT count() FROM quarantine_parts.parts
    """
    Then we get response
    """
    1
    """
    When we stop clickhouse on clickhouse02
    And we stop clickhouse on clickhouse01
    And we sleep for 5 seconds
    And we execute command on clickhouse01
    """
    mkdir -p /tmp/chadmin-test-bin
    printf '#!/bin/sh\nprintf "inactive\\n"\n' > /tmp/chadmin-test-bin/systemctl
    chmod +x /tmp/chadmin-test-bin/systemctl
    PATH=/tmp/chadmin-test-bin:$PATH chadmin --format yaml data-store detach-object-storage-parts --parts-file /tmp/quarantined-parts.json
    """
    Then we get response contains
    """
      moved: 1
    """
    And we get response contains
    """
      zookeeper_nodes: 2
    """
    And we get response contains
    """
      local_replicas:
      - clickhouse01.ch_tools_test
      replicas_to_mark_lost:
      - clickhouse02.ch_tools_test
    """
    When we execute command on clickhouse01
    """
    test -d /var/lib/clickhouse/disks/object_storage/store/200/20000000-0000-0000-0000-000000000001/detached/broken-on-start_0_0_0_0
    python3 -c "import json; manifest = json.load(open('/tmp/quarantined-parts.json')); assert manifest['dry_run'] is False; assert manifest['parts'][0]['part_name'] == '0_0_0_0'"
    ! chadmin zookeeper exists /tables/quarantine_parts/replicas/clickhouse01.ch_tools_test/parts/0_0_0_0
    ! chadmin zookeeper exists /tables/quarantine_parts/replicas/clickhouse02.ch_tools_test/parts/0_0_0_0
    test "$(chadmin zookeeper get /tables/quarantine_parts/replicas/clickhouse01.ch_tools_test/is_lost)" = 0
    test "$(chadmin zookeeper get /tables/quarantine_parts/replicas/clickhouse02.ch_tools_test/is_lost)" = 1
    """
    And we execute command on clickhouse02
    """
    supervisorctl status clickhouse-server | grep -q STOPPED
    """
    And we start clickhouse on clickhouse01
    And we execute query on clickhouse01
    """
    SELECT count() FROM system.parts WHERE database = 'quarantine_parts' AND table = 'parts' AND active
    """
    Then we get response
    """
    0
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM system.detached_parts WHERE database = 'quarantine_parts' AND table = 'parts'
    """
    Then we get response
    """
    1
    """

  Scenario: Refuse to detach object-storage parts while another replica is active.
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS quarantine_active ON CLUSTER 'cluster' SYNC;
    CREATE DATABASE quarantine_active ON CLUSTER 'cluster';
    CREATE TABLE quarantine_active.parts UUID '20000000-0000-0000-0000-000000000002' ON CLUSTER 'cluster' (n UInt32)
    ENGINE = ReplicatedMergeTree('/tables/quarantine_active', '{replica}')
    PARTITION BY n ORDER BY n SETTINGS storage_policy = 'object_storage';
    INSERT INTO quarantine_active.parts VALUES (0);
    """
    And we execute command on clickhouse01
    """
    chadmin wait replication-sync --total-timeout 10 --replica-timeout 3
    """
    When we stop clickhouse on clickhouse01
    And we sleep for 5 seconds
    And we try to execute command on clickhouse01
    """
    mkdir -p /tmp/chadmin-test-bin
    printf '#!/bin/sh\nprintf "inactive\\n"\n' > /tmp/chadmin-test-bin/systemctl
    chmod +x /tmp/chadmin-test-bin/systemctl
    PATH=/tmp/chadmin-test-bin:$PATH chadmin --format yaml data-store detach-object-storage-parts
    """
    Then it fails with response contains
    """
    Keeper replica is active for quarantine_active.parts
    """
    When we execute command on clickhouse01
    """
    table_dir=/var/lib/clickhouse/disks/object_storage/store/200/20000000-0000-0000-0000-000000000002
    test -d "$table_dir/0_0_0_0"
    test ! -e "$table_dir/detached/broken-on-start_0_0_0_0"
    chadmin zookeeper exists /tables/quarantine_active/replicas/clickhouse01.ch_tools_test/parts/0_0_0_0
    chadmin zookeeper exists /tables/quarantine_active/replicas/clickhouse02.ch_tools_test/parts/0_0_0_0
    """

  Scenario: Dry-run preserves an object-storage part and its replica metadata.
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS quarantine_dry_run ON CLUSTER 'cluster' SYNC;
    CREATE DATABASE quarantine_dry_run ON CLUSTER 'cluster';
    CREATE TABLE quarantine_dry_run.parts UUID '20000000-0000-0000-0000-000000000003' ON CLUSTER 'cluster' (n UInt32)
    ENGINE = ReplicatedMergeTree('/tables/quarantine_dry_run', '{replica}')
    PARTITION BY n ORDER BY n SETTINGS storage_policy = 'object_storage';
    INSERT INTO quarantine_dry_run.parts VALUES (0);
    """
    And we execute command on clickhouse01
    """
    chadmin wait replication-sync --total-timeout 10 --replica-timeout 3
    """
    When we stop clickhouse on clickhouse02
    And we stop clickhouse on clickhouse01
    And we sleep for 5 seconds
    And we execute command on clickhouse01
    """
    mkdir -p /tmp/chadmin-test-bin
    printf '#!/bin/sh\nprintf "inactive\\n"\n' > /tmp/chadmin-test-bin/systemctl
    chmod +x /tmp/chadmin-test-bin/systemctl
    PATH=/tmp/chadmin-test-bin:$PATH chadmin --format yaml data-store detach-object-storage-parts --dry-run --parts-file /tmp/quarantined-dry-run-parts.json
    """
    Then we get response contains
    """
      dry_run: true
    """
    And we get response contains
    """
      moved: 0
    """
    And we get response contains
    """
      zookeeper_nodes: 2
    """
    And we get response contains
    """
      local_replicas:
      - clickhouse01.ch_tools_test
      replicas_to_mark_lost:
      - clickhouse02.ch_tools_test
    """
    When we execute command on clickhouse01
    """
    table_dir=/var/lib/clickhouse/disks/object_storage/store/200/20000000-0000-0000-0000-000000000003
    test -d "$table_dir/0_0_0_0"
    test ! -e "$table_dir/detached/broken-on-start_0_0_0_0"
    python3 -c "import json; manifest = json.load(open('/tmp/quarantined-dry-run-parts.json')); assert manifest['dry_run'] is True; assert manifest['parts'][0]['part_name'] == '0_0_0_0'"
    chadmin zookeeper exists /tables/quarantine_dry_run/replicas/clickhouse01.ch_tools_test/parts/0_0_0_0
    chadmin zookeeper exists /tables/quarantine_dry_run/replicas/clickhouse02.ch_tools_test/parts/0_0_0_0
    test "$(chadmin zookeeper get /tables/quarantine_dry_run/replicas/clickhouse01.ch_tools_test/is_lost)" = 0
    test "$(chadmin zookeeper get /tables/quarantine_dry_run/replicas/clickhouse02.ch_tools_test/is_lost)" = 0
    """
    When we execute command on clickhouse01
    """
    PATH=/tmp/chadmin-test-bin:$PATH chadmin --format yaml data-store detach-object-storage-parts --dry-run --no-mark-replicas-lost
    """
    Then we get response contains
    """
      local_replicas:
      - clickhouse01.ch_tools_test
      replicas_to_mark_lost: []
    """

  Scenario Outline: Restore recoverable <filename> file for broken object storage part.
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS restore_recoverable;
    CREATE DATABASE restore_recoverable;
    CREATE TABLE restore_recoverable.file_restore (a UInt32, p UInt32) ENGINE=MergeTree() ORDER BY a PARTITION BY p SETTINGS storage_policy='object_storage', min_bytes_for_wide_part=0;
    INSERT INTO restore_recoverable.file_restore SELECT number, 0 FROM numbers(3);
    """
    And we remove s3 object for active part file <filename> from table restore_recoverable.file_restore on clickhouse01
    When we execute command on clickhouse01
    """
    chadmin --format yaml data-store detect-broken-partitions --restore-recoverable
    """
    Then we get response contains
    """
      file: <filename>
      status: restored
      detail: <detail>
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM restore_recoverable.file_restore
    """
    Then we get response
    """
    3
    """

    Examples:
    | filename                      | detail                                              |
    | checksums.txt                 | regenerated by ClickHouse ATTACH PARTITION          |
    | count.txt                     | uploaded generated content with exact metadata size |
    | default_compression_codec.txt | uploaded generated content with exact metadata size |

  # Older ClickHouse releases reject the regenerated part with
  # NO_SUCH_COLUMN_IN_TABLE when columns.txt is restored through ATTACH.
    @require_version_24.8
    Examples:
    | filename    | detail                                     |
    | columns.txt | regenerated by ClickHouse ATTACH PARTITION |

  @require_version_24.8
  Scenario: Restore all recoverable files missing from the same object storage part.
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS restore_recoverable;
    CREATE DATABASE restore_recoverable;
    CREATE TABLE restore_recoverable.file_restore (a UInt32, p UInt32) ENGINE=MergeTree() ORDER BY a PARTITION BY p SETTINGS storage_policy='object_storage', min_bytes_for_wide_part=0;
    INSERT INTO restore_recoverable.file_restore SELECT number, 0 FROM numbers(3);
    """
    And we remove s3 object for active part file checksums.txt from table restore_recoverable.file_restore on clickhouse01
    And we remove s3 object for active part file columns.txt from table restore_recoverable.file_restore on clickhouse01
    And we remove s3 object for active part file count.txt from table restore_recoverable.file_restore on clickhouse01
    And we remove s3 object for active part file default_compression_codec.txt from table restore_recoverable.file_restore on clickhouse01
    When we execute command on clickhouse01
    """
    chadmin --format yaml data-store detect-broken-partitions --restore-recoverable
    """
    Then we get response contains
    """
      file: checksums.txt
      status: restored
      detail: regenerated by ClickHouse ATTACH PARTITION
    """
    And we get response contains
    """
      file: columns.txt
      status: restored
      detail: regenerated by ClickHouse ATTACH PARTITION
    """
    And we get response contains
    """
      file: count.txt
      status: restored
      detail: uploaded generated content with exact metadata size
    """
    And we get response contains
    """
      file: default_compression_codec.txt
      status: restored
      detail: uploaded generated content with exact metadata size
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM restore_recoverable.file_restore
    """
    Then we get response
    """
    3
    """

  Scenario Outline: Restore multiple broken parts in one partition with <partition_action>.
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS restore_recoverable;
    CREATE DATABASE restore_recoverable;
    CREATE TABLE restore_recoverable.multi_part (a UInt32, p UInt32) ENGINE=MergeTree() ORDER BY a PARTITION BY p SETTINGS storage_policy='object_storage', min_bytes_for_wide_part=1000000000, min_rows_for_wide_part=1000000000;
    SYSTEM STOP MERGES restore_recoverable.multi_part;
    INSERT INTO restore_recoverable.multi_part SELECT number, 0 FROM numbers(3);
    INSERT INTO restore_recoverable.multi_part SELECT number + 3, 0 FROM numbers(3);
    """
    And we remove s3 object for active part 0 file checksums.txt from table restore_recoverable.multi_part on clickhouse01
    And we remove s3 object for active part 1 file columns.txt from table restore_recoverable.multi_part on clickhouse01
    When we execute command on clickhouse01
    """
    chadmin --format yaml data-store detect-broken-partitions --restore-recoverable <partition_action>
    """
    Then we get response contains
    """
      file: checksums.txt
      status: restored
      detail: regenerated by ClickHouse ATTACH PARTITION
    """
    And we get response contains
    """
      file: columns.txt
      status: unrecoverable
      detail: columns.txt can be restored only for Wide parts
    """
    And we get response contains
    """
      file: '*'
      status: <partition_status>
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM system.parts WHERE database='restore_recoverable' AND table='multi_part' AND active
    """
    Then we get response
    """
    <active_parts>
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM system.detached_parts WHERE database='restore_recoverable' AND table='multi_part'
    """
    Then we get response
    """
    <detached_parts>
    """

    Examples:
    | partition_action | partition_status | active_parts | detached_parts |
    | --reattach       | reattached       | 1            | 1              |
    | --detach         | detached         | 0            | 2              |

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
