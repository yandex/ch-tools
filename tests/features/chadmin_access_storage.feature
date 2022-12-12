Feature: chadmin access-storage tool

  Background:
    Given configuration
      """
      ch_user: _access_admin
      """
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
      """
    # Create test access entities.
    And we have executed queries on clickhouse01
      """
      CREATE USER IF NOT EXISTS user01;

      CREATE ROLE IF NOT EXISTS role01;

      CREATE ROW POLICY IF NOT EXISTS policy01 ON test.table_01 TO role01;

      CREATE SETTINGS PROFILE IF NOT EXISTS profile01 SETTINGS max_memory_usage = 10000000 TO role01;

      CREATE QUOTA IF NOT EXISTS quota01 FOR INTERVAL 30 minute MAX queries = 123 TO role01;
      """
    # We need to rebuild *.list files manually within restart, otherwise we have to wait ~60+ sec while CH will do that.
    And we have executed command on clickhouse01
      """
      supervisorctl restart clickhouse-server
      """

  # This scenario will simulate the following case:
  # - create access entities on `clickhouse01` only via SQL queries
  # - migrate them from `clickhouse01` to `zookeeper01` (migrate-to-replicated)
  # - migrate them from `zookeeper01` to `clickhouse02` (migrate-to-local)
  @require_version_22.3
  Scenario: migrate to replicated and back
    # === case 1: migrate-to-replicated
    When we execute command on clickhouse01
      """
      chadmin access-storage --host zookeeper01 migrate-to-replicated
      """
    # check that we have 5 entities in ZK
    When we execute ZK list query on zookeeper01
      """
      /clickhouse/access/uuid
      """
    Then we get ZK list with len 5
    # check that we have already created user in ZK
    When we execute ZK list query on zookeeper01
      """
      /clickhouse/access/U
      """
    Then we get response
      """
      user01
      """
    # check that we have already created role in ZK
    When we execute ZK list query on zookeeper01
      """
      /clickhouse/access/R
      """
    Then we get response
      """
      role01
      """
    # check that we have already created policy in ZK
    When we execute ZK list query on zookeeper01
      """
      /clickhouse/access/P
      """
    Then we get response
      """
      policy01
      """
    # check that we have already created settings profile in ZK
    When we execute ZK list query on zookeeper01
      """
      /clickhouse/access/S
      """
    Then we get response
      """
      profile01
      """
    # check that we have already created settings quota in ZK
    When we execute ZK list query on zookeeper01
      """
      /clickhouse/access/Q
      """
    Then we get response
      """
      quota01
      """
    # === case 2: migrate-to-local
    When we execute query on clickhouse02
      """
      SHOW USERS;
      """
    # there are no users on this shard yet
    Then we get response not contains user01
    When we execute command on clickhouse02
      """
      chadmin access-storage --host zookeeper01 migrate-to-local
      """
    When we execute command on clickhouse02
      """
      test -f /var/lib/clickhouse/access/need_rebuild_lists.mark && echo "exists" || echo "does not exist"
      """
    Then we get response
      """
      exists
      """
    When we have executed command on clickhouse02
      """
      supervisorctl restart clickhouse-server
      """
    And we execute command on clickhouse02
      """
      test -f /var/lib/clickhouse/access/need_rebuild_lists.mark && echo "exists" || echo "does not exist"
      """
    Then we get response
      """
      does not exist
      """
    # check that we have particular user in CH
    When we execute query on clickhouse02
      """
      SHOW USERS;
      """
    Then we get response contains user01
    # check that we have particular role in CH
    When we execute query on clickhouse02
      """
      SHOW ROLES;
      """
    Then we get response contains role01
    # check that we have particular quota in CH
    When we execute query on clickhouse02
      """
      SHOW QUOTAS;
      """
    Then we get response contains quota01
    # check that we have particular profile in CH
    When we execute query on clickhouse02
      """
      SHOW PROFILES;
      """
    Then we get response contains profile01
    # check that we have particular row policy in CH
    When we execute query on clickhouse02
      """
      SHOW ROW POLICIES;
      """
    Then we get response contains policy01
