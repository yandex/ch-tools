Feature: chadmin access-storage tool

  Background:
    Given configuration
      """
      ch_user: _access_admin
      """
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    # And a working clickhouse on clickhouse02
    # Create test data set.
    Given we have executed queries on clickhouse01
      """
      CREATE USER IF NOT EXISTS test_user ON CLUSTER 'cluster';
      """

  @require_version_22.3
  Scenario: migrate to replicated
    When we execute command on clickhouse01
      """
      chadmin access-storage --host zookeeper01 migrate-to-replicated
      """
    Then we get response
      """
      0;OK
      """
    # todo: change query format
    When we execute ls ZK query on zookeeper01
      """
      /clickhouse/access/U
      """
    Then we get zk response
      """
      test_user
      """
