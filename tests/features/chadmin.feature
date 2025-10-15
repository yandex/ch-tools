Feature: chadmin commands.

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario: Sanity check
    Given we have executed queries on clickhouse01
    # force creation of system log tables
    """
    SYSTEM FLUSH LOGS ON CLUSTER '{cluster}';
    """
    When we execute command on clickhouse01
    """
    chadmin config
    """
    And we execute command on clickhouse01
    """
    chadmin settings
    """
    And we execute command on clickhouse01
    """
    chadmin functions
    """
    And we execute command on clickhouse01
    """
    chadmin metrics
    """
    And we execute command on clickhouse01
    """
    chadmin async-metrics
    """
    And we execute command on clickhouse01
    """
    chadmin events
    """
    And we execute command on clickhouse01
    """
    chadmin query-log list
    """
    And we execute command on clickhouse01
    """
    chadmin query-log list --cluster --limit 10
    """
    And we execute command on clickhouse01
    """
    chadmin part-log list
    """
    And we execute command on clickhouse01
    """
    chadmin part-log list --cluster --limit 10
    """
    And we execute command on clickhouse01
    """
    chadmin database list
    """
    And we execute command on clickhouse01
    """
    chadmin table list
    """
    And we execute command on clickhouse01
    """
    chadmin replica list
    """
    And we execute command on clickhouse01
    """
    chadmin dictionary list
    """
    And we execute command on clickhouse01
    """
    chadmin diagnostics
    """
    Then it completes successfully

  @require_version_24.2
  Scenario Outline: Check set-flag with convert_to_replicated
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS db1;
    CREATE TABLE IF NOT EXISTS db1.table_01 (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n;
    CREATE TABLE IF NOT EXISTS db1.table_02 (n Int32)
    ENGINE = MergeTree() PARTITION BY n ORDER BY n;

    CREATE DATABASE IF NOT EXISTS db2;
    CREATE TABLE IF NOT EXISTS db2.table_03 (n Int32)
    ENGINE = MergeTree() PARTITION BY n ORDER BY n;
    CREATE TABLE IF NOT EXISTS db2.table_04 (n Int32)
    ENGINE = MergeTree() PARTITION BY n ORDER BY n;
    """
    When we execute command on clickhouse01
    """
    chadmin table set-flag <options> -v --engine %MergeTree --exclude-engine Replicated% convert_to_replicated
    """
    And we execute command on clickhouse01
    """
    supervisorctl restart clickhouse-server
    """
    Given a working clickhouse on clickhouse01
    When we execute query on clickhouse01
    """
    SELECT name, engine FROM system.tables WHERE database LIKE 'db%'
    """
    Then we get query response
    """
    <result>
    """
    Examples:
      | options                                              | result                                                                                                                      |
      | --all --exclude-database=system                      | table_01\tReplicatedMergeTree\ntable_02\tReplicatedMergeTree\ntable_03\tReplicatedMergeTree\ntable_04\tReplicatedMergeTree  |
      | --database=db1,db2                                   | table_01\tReplicatedMergeTree\ntable_02\tReplicatedMergeTree\ntable_03\tReplicatedMergeTree\ntable_04\tReplicatedMergeTree  |
      | --database=db1,db2        --table=table_02,table_03  | table_01\tReplicatedMergeTree\ntable_02\tReplicatedMergeTree\ntable_03\tReplicatedMergeTree\ntable_04\tMergeTree            |
      | --database=db1,db2        --exclude-table=table_04   | table_01\tReplicatedMergeTree\ntable_02\tReplicatedMergeTree\ntable_03\tReplicatedMergeTree\ntable_04\tMergeTree            |
      | --database=db1,db2        --table=table_03           | table_01\tReplicatedMergeTree\ntable_02\tMergeTree\ntable_03\tReplicatedMergeTree\ntable_04\tMergeTree                      |
      | --database=db2                                       | table_01\tReplicatedMergeTree\ntable_02\tMergeTree\ntable_03\tReplicatedMergeTree\ntable_04\tReplicatedMergeTree            |
      | --database=db2            --table=table_01           | table_01\tReplicatedMergeTree\ntable_02\tMergeTree\ntable_03\tMergeTree\ntable_04\tMergeTree                                |

  Scenario Outline: Check wait replication sync
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test ON CLUSTER 'cluster';

    CREATE TABLE IF NOT EXISTS test.table_01 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n;
    """
    When we execute command on clickhouse01
    """
    chadmin wait replication-sync --total-timeout 10 --replica-timeout 3 -p 1 -w 4 <options>
    """
    When we execute query on clickhouse01
    """
    SYSTEM STOP FETCHES
    """
    And we execute query on clickhouse02
    """
    INSERT INTO test.table_01 SELECT number FROM numbers(100)
    """
    And we sleep for 5 seconds
    When we try to execute command on clickhouse01
    """
    chadmin wait replication-sync --total-timeout 10 --replica-timeout 3 -p 1 -w 4 <options>
    """
    Then it fails with response contains
    """
    Read timeout while running query.
    """
    When we execute query on clickhouse01
    """
    SYSTEM START FETCHES
    """
    When we execute command on clickhouse01
    """
    supervisorctl stop clickhouse-server
    """
    When we try to execute command on clickhouse01
    """
    chadmin wait replication-sync --total-timeout 10 --replica-timeout 3 -p 1 -w 4 <options>
    """
    Then it fails with response contains
    """
    Connection error
    """
    When we execute command on clickhouse01
    """
    supervisorctl start clickhouse-server
    """
    When we execute command on clickhouse01
    """
    chadmin wait replication-sync --total-timeout 10 --replica-timeout 3 -p 1 -w 4 <options>
    """
    Examples:
      | options             |
      |                     |
      | --lightweight       |
      | --full              |

 
  Scenario: Check wait replication sync for replicated database
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE testdb ON CLUSTER 'cluster' ENGINE  = Replicated('/clickhouse/databases/test', 'shard1', '{replica}');
    """
    When we execute query on clickhouse02
    """
    CREATE TABLE IF NOT EXISTS testdb.table_01 (n Int32) ENGINE = MergeTree() ORDER BY n
    """
    When we execute command on clickhouse01
    """
    supervisorctl stop clickhouse-server
    """
    When we try to execute command on clickhouse01
    """
    chadmin wait replication-sync --total-timeout 10 --replica-timeout 3 -p 1 -w 4
    """
    Then it fails with response contains
    """
    Connection error
    """
    When we execute command on clickhouse01
    """
    supervisorctl start clickhouse-server
    """
    When we execute command on clickhouse01
    """
    chadmin wait replication-sync --total-timeout 10 --replica-timeout 3 -p 1 -w 4 --sync-databases
    """
    When we execute query on clickhouse01
    """
    SELECT * from testdb.table_01
    """
    

