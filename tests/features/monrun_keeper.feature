Feature: keeper-monitoring tool

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01

  Scenario: Check Zookeeper alive with keeper monitoring
    When we execute command on zookeeper01
    """
    keeper-monitoring -n alive
    """
    Then we get response
    """
    0;OK
    """
    When we execute command on zookeeper01
    """
    supervisorctl stop zookeeper
    """
    When we execute command on zookeeper01
    """
    keeper-monitoring -n alive
    """
    Then we get response
    """
    2;KazooTimeoutError('Connection time-out')
    """

  Scenario: Check ZooKeeper version
    When we execute command on zookeeper01
    """
    keeper-monitoring version
    """
    Then we get response contains
    """
    0;
    """
    When we execute command on zookeeper01
    """
    supervisorctl stop zookeeper
    """
    When we execute command on zookeeper01
    """
    keeper-monitoring version
    """
    Then we get response
    """
    1;ConnectionRefusedError(111, 'Connection refused')
    """

  Scenario: Check CH keeper alive with keeper monitoring
    Given a working keeper on clickhouse01
    When we execute command on clickhouse01
    """
    keeper-monitoring -n alive
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
    """
    keeper-monitoring -n alive
    """
    Then we get response
    """
    2;KazooTimeoutError('Connection time-out')
    """
