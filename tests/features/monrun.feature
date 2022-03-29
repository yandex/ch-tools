Feature: ch-monitoring tool

  Background:
    Given default configuration
    And a working s3
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

    CREATE TABLE test.test_unfreeze (id int, name String) ENGINE=MergeTree() ORDER BY id SETTINGS storage_policy='object_storage';
    INSERT INTO test.test_unfreeze VALUES(5, 'hello');
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

  Scenario: Check Log errors
    When we sleep for 30 seconds
    And we execute command on clickhouse01
    """
    ch-monitoring log-errors -n 20
    """
    Then we get response
    """
    0;OK, 0 errors for last 20 seconds
    """
    When we execute query on clickhouse01
    """
    SELECT 1;
    """
    And we sleep for 5 seconds
    And we execute command on clickhouse01
    """
    ch-monitoring log-errors -n 20
    """
    Then we get response
    """
    0;OK, 0 errors for last 20 seconds
    """
    When we execute query on clickhouse01
    """
    FOOBAR INCORRECT REQUEST;
    """
    And we sleep for 5 seconds
    And we execute command on clickhouse01
    """
    ch-monitoring log-errors -n 20
    """
    Then we get response
    """
    0;OK, 2 errors for last 20 seconds
    """
    When we execute query on clickhouse01
    """
    FOOBAR INCORRECT REQUEST;
    """
    And we execute query on clickhouse01
    """
    FOOBAR INCORRECT REQUEST;
    """
    And we execute query on clickhouse01
    """
    FOOBAR INCORRECT REQUEST;
    """
    And we execute query on clickhouse01
    """
    FOOBAR INCORRECT REQUEST;
    """
    And we sleep for 5 seconds
    And we execute command on clickhouse01
    """
    ch-monitoring log-errors -n 20
    """
    Then we get response
    """
    1;10 errors for last 20 seconds
    """
    When we sleep for 30 seconds
    And we execute command on clickhouse01
    """
    ch-monitoring log-errors -n 20
    """
    Then we get response
    """
    0;OK, 0 errors for last 20 seconds
    """

  Scenario: Check Log errors with timetail stderr output
    When we execute command on clickhouse01
    """
    echo 2000.01.01 00:00:00 test line > /tmp/test.log
    for j in {1..2000}; do echo junk line >> /tmp/test.log; done
    ch-monitoring log-errors -n 20 -f /tmp/test.log
    """
    Then we get response
    """
    0;OK, 0 errors for last 20 seconds
    """

  Scenario: Check Ping
    When we execute command on clickhouse01
    """
    ch-monitoring ping
    """
    Then we get response
    """
    0;OK
    """
    When we execute command on clickhouse01
    """
    supervisorctl stop clickhouse-server
    ch-monitoring ping
    """
    Then we get response contains
    """
    2;ClickHouse is dead
    """

  @require_version_21.3
  Scenario: Check Orphaned Backups
    When we execute command on clickhouse01
     """
     ch-monitoring orphaned-backups
     """
    Then we get response
     """
     0;OK
     """
    When we execute query on clickhouse01
     """
     ALTER TABLE test.test_unfreeze FREEZE;
     """
    And we execute command on clickhouse01
     """
     ch-monitoring orphaned-backups
     """
    Then we get response contains
     """
     1;There are 1 orphaned S3 backups
     """

  Scenario: Check restore errors
    When we execute command on clickhouse01
    """
    echo '{
      "failed":{
        "failed_parts":{
          "db1": {
            "tbl1": {
              "failed1":"exception1"
            }
          }
        }
      },
      "databases": {
        "db1": {
          "tbl1": ["part1", "part2", "part3", "part4", "part5"]
        },
        "db2": {
          "tbl2": ["part1", "part2", "part3", "part4", "part5"]
        }
      }
    }' > /tmp/ch_backup_restore_state.json
    """
    When we execute command on clickhouse01
    """
    ch-monitoring backup
    """
    Then we get response
    """
    1;Some parts restore failed: 1(9%)
    """
    When we execute command on clickhouse01
    """
    echo '{
      "failed":{
        "failed_parts":{
          "db1": {
            "tbl1": {
              "failed1":"exception1"
            }
          },
          "db2": {
            "tbl2": {
              "failed2":"exception2"
            }
          }
        }
      },
      "databases": {
        "db2": {
          "tbl2": ["part1"]
        }
      }
    }' > /tmp/ch_backup_restore_state.json
    """
    When we execute command on clickhouse01
    """
    ch-monitoring backup
    """
    Then we get response
    """
    2;Some parts restore failed: 2(66%)
    """

  @require_version_21.8
  Scenario: Check CH Keeper alive
    Given a working keeper on clickhouse01
    When we execute command on clickhouse01
     """
     ch-monitoring keeper
     """
    Then we get response
     """
     0;OK
     """
    When we execute command on clickhouse01
    """
    supervisorctl stop clickhouse-server
    """
    When we execute command on clickhouse01
    """"
    ch-monitoring keeper
    """
    Then we get response contains
    """
    2;KazooTimeoutError('Connection time-out')
    """
