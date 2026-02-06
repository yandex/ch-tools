Feature: chadmin server commands

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And merged clickhouse-tools configuration on clickhouse01
    """
    chadmin:
      server:
        restart:
          command: supervisorctl restart clickhouse-server
      wait:
        ping_command: ch-monitoring ping
    """

  Scenario: Restart ClickHouse server and verify it's operational
    When we execute command on clickhouse01
    """
    chadmin server restart --timeout 30
    """
    Then it completes successfully
    When we execute query on clickhouse01
    """
    SELECT uptime()
    """
    Then we get response with uptime less than 30 seconds
    When we execute query on clickhouse01
    """
    SELECT 1
    """
    Then query was completed successfully
    When we execute query on clickhouse01
    """
    SELECT count() FROM system.dictionaries WHERE status IN ('LOADING', 'LOADED_AND_RELOADING')
    """
    Then we get response
    """
    0
    """
    