Feature: chadmin replica commands.

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario: Check drop replica with zero-copy locks
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
    Then the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3 is equal to
    """
    /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001
    /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000002
    """
    When we execute queries on clickhouse01
    """
    DETACH TABLE test.table_01;
    """
    And we execute command on clickhouse01
    """
    chadmin replica drop clickhouse01.ch_tools_test /tables/table_01
    """
    Then the list of children on clickhouse01 for zk node /tables/table_01/replicas is equal to
    """
    /tables/table_01/replicas/clickhouse02.ch_tools_test
    """
    And the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3 is equal to
    """
    /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000001
    /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000002
    """
    When we execute queries on clickhouse02
    """
    DETACH TABLE test.table_01;
    """
    ANd we execute command on clickhouse01
    """
    chadmin replica drop clickhouse02.ch_tools_test /tables/table_01
    """
    Then the list of children on clickhouse01 for zk node /tables is equal to
    """
    /tables/table_02
    """
    And the list of children on clickhouse01 for zk node /clickhouse/zero_copy/zero_copy_s3 is equal to
    """
    /clickhouse/zero_copy/zero_copy_s3/10000000-0000-0000-0000-000000000002
    """
 
  Scenario Outline: Check replica restore (<replicas_count> replicas, <workers> workers) 
    Given populated clickhouse with <replicas_count> replicated tables on clickhouse01 with db database and table_ prefix
    When we delete zookeepers nodes /db on clickhouse01
    And we execute command on clickhouse01
    """
    supervisorctl restart clickhouse-server
    """
    Then a clickhouse will be worked on clickhouse01
    And <replicas_count> readonly replicas on clickhouse01

    When we execute command on clickhouse01
    """
    chadmin replica restore --all -w <workers>
    """
    Then 0 readonly replicas on clickhouse01
  Examples:
      | replicas_count | workers|
      | 5              | 1      |
      | 12             | 4      |
      | 30             | 12     |

  Scenario: Check replica restore with broken part
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test ON CLUSTER 'cluster';

    CREATE TABLE IF NOT EXISTS test.table_01 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n;

    INSERT INTO test.table_01 SELECT number FROM numbers(5);
    """
    When we execute command on clickhouse01
    """
    chadmin zookeeper delete /tables/table_01 && chadmin replica restart -t table_01
    """
    And we execute command on clickhouse01
    """
    rm /var/lib/clickhouse/data/test/table_01/2_0_0_0/columns.txt
    """
    And we execute command on clickhouse01
    """
    chadmin replica restore -t table_01
    """
    Then we get response contains
    """
    No columns.txt in part 2_0_0_0
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM system.parts WHERE table = 'table_01' and database = 'test'
    """
    Then we get response
    """
    4
    """
    When we execute command on clickhouse02
    """
    chadmin replica restart -t table_01 && chadmin replica restore -t table_01
    """
    And we execute query on clickhouse02
    """
    SYSTEM SYNC REPLICA test.table_01
    """
    And we execute query on clickhouse02
    """
    SELECT count() FROM system.parts WHERE table = 'table_01' and database = 'test'
    """
    Then we get response
    """
    4
    """
