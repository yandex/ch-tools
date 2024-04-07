Feature: chadmin commands.

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test ON CLUSTER 'cluster';

    CREATE TABLE IF NOT EXISTS test.table_01 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n;
    """

  @require_version_24.2
  Scenario Outline: Check set-flag with convert_to_replicated
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test1;

    CREATE TABLE IF NOT EXISTS test.table_02 (n Int32)
    ENGINE = MergeTree() PARTITION BY n ORDER BY n;

    CREATE TABLE IF NOT EXISTS test1.table_03 (n Int32)
    ENGINE = MergeTree() PARTITION BY n ORDER BY n;
    CREATE TABLE IF NOT EXISTS test1.table_04 (n Int32)
    ENGINE = MergeTree() PARTITION BY n ORDER BY n;
    """
    When we execute command on clickhouse01
    """
    chadmin table set-flag <options> --engine %MergeTree --exclude-engine Replicated% convert_to_replicated
    """
    And we execute command on clickhouse01
    """
    supervisorctl restart clickhouse-server
    """
    Given a working clickhouse on clickhouse01
    When we execute query on clickhouse01
    """
    SELECT name, engine FROM system.tables WHERE database LIKE 'test%'
    """
    Then we get query response
    """
    <result>
    """
    Examples:
      | options                                                 | result                                                                                                                      |
      | --all --exclude-database=system                         | table_01\tReplicatedMergeTree\ntable_02\tReplicatedMergeTree\ntable_03\tReplicatedMergeTree\ntable_04\tReplicatedMergeTree  |
      | --database=test,test1                                   | table_01\tReplicatedMergeTree\ntable_02\tReplicatedMergeTree\ntable_03\tReplicatedMergeTree\ntable_04\tReplicatedMergeTree  |
      | --database=test,test1        --table=table_02,table_03  | table_01\tReplicatedMergeTree\ntable_02\tReplicatedMergeTree\ntable_03\tReplicatedMergeTree\ntable_04\tMergeTree            |
      | --database=test,test1        --exclude-table=table_04   | table_01\tReplicatedMergeTree\ntable_02\tReplicatedMergeTree\ntable_03\tReplicatedMergeTree\ntable_04\tMergeTree            |
      | --database=test,test1        --table=table_03           | table_01\tReplicatedMergeTree\ntable_02\tMergeTree\ntable_03\tReplicatedMergeTree\ntable_04\tMergeTree                      |
      | --database=test1                                        | table_01\tReplicatedMergeTree\ntable_02\tMergeTree\ntable_03\tReplicatedMergeTree\ntable_04\tReplicatedMergeTree            |
      | --database=test1             --table=table_01           | table_01\tReplicatedMergeTree\ntable_02\tMergeTree\ntable_03\tMergeTree\ntable_04\tMergeTree                                |

  Scenario: Check wait replication sync
    When we execute command on clickhouse01
    """
    chadmin wait replication-sync --total-timeout 10 --replica-timeout 3 -p 1 -w 4
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
    chadmin wait replication-sync --total-timeout 10 --replica-timeout 3 -p 1 -w 4
    """
    Then it fails with response contains
    """
    Timeout while running SYNC REPLICA on
    """
    When we execute query on clickhouse01
    """
    SYSTEM START FETCHES
    """
    When we execute command on clickhouse01
    """
    chadmin wait replication-sync --total-timeout 10 --replica-timeout 3 -p 1 -w 4
    """
