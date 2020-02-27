Feature: ch-resetup tool

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario: Resetup ClickHouse host
    # Create test data set.
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test ON CLUSTER 'cluster';

    CREATE TABLE test.table_01 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n;

    INSERT INTO test.table_01 (n) SELECT number FROM system.numbers LIMIT 10;
    """
    # Simulate resetup of ClickHouse host.
    And we have executed command on clickhouse01
    """
    supervisorctl stop clickhouse-server
    """
    And we have executed command on clickhouse01
    """
    rm -rf /var/lib/clickhouse/*
    """
    # Execute ch-resetup (start of clickhouse-server must be performed inside the script).
    When we execute command on clickhouse01
    """
    supervisorctl start clickhouse-server
    """
    And we execute command on clickhouse01
    """
    ch-resetup --insecure --service-manager supervisord
    """
    Then clickhouse01 has the same schema as clickhouse02
    And clickhouse01 has the same data as clickhouse02