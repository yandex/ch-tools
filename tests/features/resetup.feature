Feature: ch-resetup tool

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    # Create test data set.
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test ON CLUSTER 'cluster';

    CREATE TABLE test.table_01 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n;

    CREATE TABLE test.dtable_01 ON CLUSTER 'cluster' AS test.table_01
    ENGINE = Distributed('cluster', 'test', 'table_01', n);

    INSERT INTO test.dtable_01 (n) SELECT number FROM system.numbers LIMIT 10;
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

  Scenario: Resetup ClickHouse host with zk metadata
    # Execute ch-resetup (start of clickhouse-server must be performed inside the script).
    When we execute command on clickhouse01
    """
    supervisorctl start clickhouse-server
    """
    And we execute command on clickhouse01
    """
    ch-resetup --insecure --service-manager supervisord --zk-root '/'
    """
    When we sleep for 10 seconds
    Then clickhouse01 has the same schema as clickhouse02
    And clickhouse01 has the same data as clickhouse02

  Scenario: Resetup ClickHouse host without zk metadata
    Given we have removed ZK metadata for clickhouse01
    # Execute ch-resetup (start of clickhouse-server must be performed inside the script).
    When we execute command on clickhouse01
    """
    supervisorctl start clickhouse-server
    """
    And we execute command on clickhouse01
    """
    ch-resetup --insecure --service-manager supervisord --zk-root '/'
    """
    When we sleep for 10 seconds
    Then clickhouse01 has the same schema as clickhouse02
    And clickhouse01 has the same data as clickhouse02

  Scenario: Resetup fails if there are user data
    When we try to execute command on clickhouse02
    """
    ch-resetup --insecure --service-manager supervisord
    """
    Then it fails

  Scenario: Resetup ClickHouse host with custom port settings and not empty DDL queue
    Given we have executed query on clickhouse02
    """
    CREATE DATABASE test2 ON CLUSTER 'cluster';
    """
    And we get reponse code 500
    And we have executed query on clickhouse02
    """
    CREATE TABLE test2.table_02 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/table_02', '{replica}') PARTITION BY n ORDER BY n;
    """
    And we get reponse code 500
    When we execute command on clickhouse01
    """
    echo "
    <yandex>
          <http_port>28123</http_port>
          <https_port>29443</https_port>
          <tcp_port>29000</tcp_port>
          <tcp_port_secure>29440</tcp_port_secure>
          <distributed_ddl>
              <path>/clickhouse/task_queue/fake_ddl</path>
          </distributed_ddl>
    </yandex>" > /etc/clickhouse-server/config.d/custom.xml
    """
    And we execute command on clickhouse01
    """
    supervisorctl start clickhouse-server
    """
    And we execute command on clickhouse01
    """
    ch-resetup --insecure --service-manager supervisord --zk-root '/' --port 29443
    """
    And we sleep for 10 seconds
    And we execute command on clickhouse01
    """
    rm /etc/clickhouse-server/config.d/custom.xml
    """
    And we execute command on clickhouse01
    """
    supervisorctl restart clickhouse-server
    """
    Then clickhouse01 has the same schema as clickhouse02
    And clickhouse01 has the same data as clickhouse02
