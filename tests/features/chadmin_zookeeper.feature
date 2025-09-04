Feature: chadmin zookeeper commands.

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02


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

  Scenario: Cleanup all zero-copy locks for table with custom zookeeper zero-copy path
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
    
  Scenario: Cleanup all hosts
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS test ON CLUSTER 'cluster'; 
    CREATE DATABASE test ON CLUSTER 'cluster';

    CREATE TABLE test.table_01 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n;

    CREATE TABLE test.table_02 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_02', '{replica}') PARTITION BY n ORDER BY n;

    DETACH TABLE test.table_01 ON CLUSTER 'cluster';
    DETACH TABLE test.table_02 ON CLUSTER 'cluster';
    """

    And we do hosts cleanup on clickhouse01 with fqdn clickhouse01.ch_tools_test,clickhouse02.ch_tools_test and zk root /
    Then the list of children on clickhouse01 for zk node /tables/ is empty

  Scenario: Cleanup all hosts dry run
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS test ON CLUSTER 'cluster'; 
    CREATE DATABASE test ON CLUSTER 'cluster';

    CREATE TABLE test.table_01 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n;

    CREATE TABLE test.table_02 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_02', '{replica}') PARTITION BY n ORDER BY n;

    DETACH TABLE test.table_01 ON CLUSTER 'cluster';
    DETACH TABLE test.table_02 ON CLUSTER 'cluster';
    """

    And we do hosts dry cleanup on clickhouse01 with fqdn clickhouse01.ch_tools_test,clickhouse02.ch_tools_test and zk root /
    Then the list of children on clickhouse01 for zk node /tables/ is equal to
    """
    /tables/table_01
    /tables/table_02
    """

  Scenario: Cleanup single host 
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS test ON CLUSTER 'cluster'; 
    CREATE DATABASE test ON CLUSTER 'cluster';

    CREATE TABLE test.table_01 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n;

    CREATE TABLE test.table_02 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_02', '{replica}') PARTITION BY n ORDER BY n;

    DETACH TABLE test.table_01 ON CLUSTER 'cluster';
    DETACH TABLE test.table_02 ON CLUSTER 'cluster';
    """

    And we do hosts cleanup on clickhouse01 with fqdn clickhouse01.ch_tools_test and zk root /
    Then the list of children on clickhouse01 for zk node /tables/table_01/replicas is equal to
    """
    /tables/table_01/replicas/clickhouse02.ch_tools_test
    """
    And the list of children on clickhouse01 for zk node /tables/table_02/replicas is equal to
    """
    /tables/table_02/replicas/clickhouse02.ch_tools_test
    """

  Scenario: Cleanup single host on non empty node.
    When we execute queries on clickhouse02
    """
    DROP DATABASE IF EXISTS test ON CLUSTER 'cluster'; 
    CREATE DATABASE test ON CLUSTER 'cluster';

    CREATE TABLE test.table_01 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n;

    CREATE TABLE test.table_02 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_02', '{replica}') PARTITION BY n ORDER BY n;

    DETACH TABLE test.table_01;
    DETACH TABLE test.table_02;
    """

    And we do hosts cleanup on clickhouse01 with fqdn clickhouse02.ch_tools_test and zk root /
    Then the list of children on clickhouse02 for zk node /tables/table_01/replicas is equal to
    """
    /tables/table_01/replicas/clickhouse01.ch_tools_test
    """
    And the list of children on clickhouse02 for zk node /tables/table_02/replicas is equal to
    """
    /tables/table_02/replicas/clickhouse01.ch_tools_test
    """

  @require_version_23.1
  Scenario: Remove single host from Replicated database.
    When we execute queries on clickhouse01
    """
    CREATE DATABASE testdb ON CLUSTER 'cluster' ENGINE  = Replicated('/clickhouse/databases/test', 'shard1', '{replica}');
    DETACH DATABASE testdb ON CLUSTER 'cluster';
    """
    And we do hosts cleanup on clickhouse01 with fqdn clickhouse01.ch_tools_test and zk root /
    Then the list of children on clickhouse01 for zk node /clickhouse/databases/test/replicas/ is equal to
    """
    /clickhouse/databases/test/replicas/shard1|clickhouse02.ch_tools_test
    """

  @require_version_23.1
  Scenario: Remove all host from Replicated database.
    When we execute queries on clickhouse01
    """
    CREATE DATABASE testdb ON CLUSTER 'cluster' ENGINE = Replicated('/clickhouse/databases/test', 'shard1', '{replica}');
    DETACH DATABASE testdb ON CLUSTER 'cluster';
    """
    And we do hosts cleanup on clickhouse01 with fqdn clickhouse01.ch_tools_test,clickhouse02.ch_tools_test and zk root /
    Then the list of children on clickhouse01 for zk node /clickhouse/databases/ is empty

  Scenario: Zookeeper recursive delete command
    When we execute chadmin create zk nodes on clickhouse01
    """
      /test/a/b/c
      /test/a/b/d
      /test/a/f
    """
    And we delete zookeepers nodes /test/a on clickhouse01
    Then the list of children on clickhouse01 for zk node /test is empty

  Scenario: Zookeeper delete parent and child nodes
    When we execute chadmin create zk nodes on clickhouse01
    """
      /test/a/b
      /test/c/d
    """
    # Make sure that it is okey to delete the node and its child.
    And we delete zookeepers nodes /test/a,/test/a/b on clickhouse01
    And we delete zookeepers nodes /test/c/d,/test/c on clickhouse01
    Then the list of children on clickhouse01 for zk node /test is empty


  Scenario: Set finished to ddl with removed host.
    # Create the fake cluster with removed host.
    When we put the  clickhouse config to path /etc/clickhouse-server/config.d/cluster.xml with restarting on clickhouse02
    """
    <clickhouse>
        <remote_servers>
          <cluster_with_removed_host>
              <shard>
                  <internal_replication>true</internal_replication>
                  <replica>
                      <host>clickhouse02</host>
                      <port>9000</port>
                  </replica>

                  <replica>
                      <host>zone-host.db.asd.net</host>
                      <port>9000</port>
                  </replica>
              </shard>
          </cluster_with_removed_host>
      </remote_servers>
    </clickhouse>
    """
    And we execute query on clickhouse02
    """
      CREATE DATABASE test_db ON CLUSTER 'cluster_with_removed_host'
    """
    # Make sure that ddl have executed on clickhouse02
    And we sleep for 5 seconds
    And we execute query on clickhouse02
    """
      CREATE TABLE test_db.test ON CLUSTER 'cluster_with_removed_host'  (a int) ENGINE=MergeTree() ORDER BY a
    """
  
    And we do hosts cleanup on clickhouse02 with fqdn zone-host.db.asd.net
    Then there are no unfinished dll queries on clickhouse02
    # It is not necessary, just to do not increase sleep time in nexts steps.
    # We are doing it to reload DDL queue in clickhouse-server
    When we execute command on clickhouse01
    """
    supervisorctl restart clickhouse-server
    """
    When we sleep for 15 seconds
    And we execute command on clickhouse01
    """
    ch-monitoring log-errors -n 5
    """
    Then we get response
    """
    0;OK, 0 errors for last 5 seconds
    """
