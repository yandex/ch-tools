Feature: keeper-monitoring tool

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper with TLS
    And a working clickhouse on clickhouse01

  Scenario: Check TLS Zookeeper alive with keeper monitoring
    When we execute command on zookeeper01
    """
    keeper-monitoring alive
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
    keeper-monitoring alive
    """
    Then we get response
    """
    2;KazooTimeoutError('Connection time-out')
    """

  Scenario: Check TLS ZooKeeper version
    When we execute command on zookeeper01
    """
    keeper-monitoring version
    """
    Then we get response
    """
    0;3.4.8-1--1, built on Fri, 26 Feb 2016 14:51:43 +0100
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

  @require_version_22.8
  Scenario: Check TLS CH keeper alive with keeper monitoring
    Given a working keeper with TLS on clickhouse01
    When we execute command on clickhouse01
    """
    keeper-monitoring alive
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
    keeper-monitoring alive
    """
    Then we get response
    """
    2;KazooTimeoutError('Connection time-out')
    """
