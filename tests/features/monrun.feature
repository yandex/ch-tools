Feature: ch-monitorung tool

  Background:
    Given default configuration
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    # Create test data set.
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test ON CLUSTER 'cluster';

    CREATE TABLE IF NOT EXISTS test.table_01 ON CLUSTER 'cluster' (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/table_01', '{replica}') PARTITION BY n ORDER BY n;

    CREATE TABLE IF NOT EXISTS test.dtable_01 ON CLUSTER 'cluster' AS test.table_01
    ENGINE = Distributed('cluster', 'test', 'table_01', n);

    INSERT INTO test.dtable_01 (n) SELECT number FROM system.numbers LIMIT 10;
    """

  Scenario: Check Readonly replica
    When we execute command on clickhouse01
    """
    ch-monitoring ro-replica
    """
    Then we get response
    """
    0;OK
    """
    When we execute command on zookeeper01
    """
    kill 1
    """
    And we sleep for 10 seconds
    And we execute command on clickhouse01
    """
    ch-monitoring ro-replica
    """
    Then we get response
    """
    2;Readonly replica tables: [['test', 'table 01']]
    """

  Scenario: Check CoreDumps
    When we execute command on clickhouse01
    """
    ch-monitoring core-dumps
    """
    Then we get response
    """
    1;Core dump directory does not exist: /var/cores
    """
    When we execute command on clickhouse01
    """
    mkdir /var/cores
    """
    When we execute command on clickhouse01
    """
    ch-monitoring core-dumps
    """
    Then we get response
    """
    0;OK
    """
    When we execute command on clickhouse01
    """
    echo 1 > /var/cores/fakecore
    """
    And we execute command on clickhouse01
    """
    ch-monitoring core-dumps
    """
    Then we get response
    """
    0;OK
    """
    When we execute command on clickhouse01
    """
    chown clickhouse /var/cores/fakecore
    """
    And we execute command on clickhouse01
    """
    ch-monitoring core-dumps
    """
    Then we get response contains
    """
    2;/var/cores/fakecore
    """

  Scenario: Check Geobase
    When we execute command on clickhouse01
    """
    ch-monitoring geobase
    """
    Then we get response contains
    """
    2;HTTPError('500 Server Error: Internal Server Error
    """
    When we execute command on clickhouse01
    """
    echo -e "
        <yandex>
            <path_to_regions_hierarchy_file>/opt/yandex/clickhouse-geodb/regions_hierarchy.txt</path_to_regions_hierarchy_file>
            <path_to_regions_names_files>/opt/yandex/clickhouse-geodb/</path_to_regions_names_files>
        </yandex>
        " > /etc/clickhouse-server/config.d/geo.xml && \
    supervisorctl restart clickhouse-server
    """
    And we sleep for 10 seconds
    And we execute command on clickhouse01
    """
    ch-monitoring geobase
    """
    Then we get response
    """
    0;OK
    """

  Scenario: Check Distributed tables
    When we execute command on clickhouse01
    """
    ch-monitoring dist-tables
    """
    Then we get response
    """
    0;OK
    """

  Scenario: Check Replication lag
    When we execute command on clickhouse01
    """
    ch-monitoring replication-lag
    """
    Then we get response
    """
    0;OK
    """

  Scenario: Check System queues size
    When we execute command on clickhouse01
    """
    ch-monitoring system-queues
    """
    Then we get response
    """
    0;OK
    """

#  Scenario: Check Resetup state
#    When we execute command on clickhouse01
#    """
#    ch-monitoring resetup-state
#    """
#    Then we get response
#    """
#    0;OK
#    """
