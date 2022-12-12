# This feature will simulate the following case:
# - create access entities only on `clickhouse01` via SQL queries
# - migrate them from `clickhouse01` to `zookeeper01` (use migrate-to-replicated command)
# - migrate them from `zookeeper01` to `clickhouse02` (use migrate-to-local command)
@dependent-scenarios
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
    And we have executed queries on clickhouse01
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

  # pre-check that we don't have any access entities in ZK yet
  @require_version_22.3
  Scenario: pre-check zookeeper01's data
    # check UUID
    When we execute ZK list query on zookeeper01
    """
    /clickhouse/access/uuid
    """
    Then we get ZK list with len 0
    # check U
    When we execute ZK list query on zookeeper01
    """
    /clickhouse/access/U
    """
    Then we get ZK list with len 0
    # check R
    When we execute ZK list query on zookeeper01
    """
    /clickhouse/access/R
    """
    Then we get ZK list with len 0
    # check P
    When we execute ZK list query on zookeeper01
    """
    /clickhouse/access/P
    """
    Then we get ZK list with len 0
    # check S
    When we execute ZK list query on zookeeper01
    """
    /clickhouse/access/S
    """
    Then we get ZK list with len 0
    # check Q
    When we execute ZK list query on zookeeper01
    """
    /clickhouse/access/Q
    """
    Then we get ZK list with len 0

  # pre-check that we don't have any access entities in CH on 2nd shard yet
  @require_version_22.3
  Scenario: pre-check clickhouse02's data
    # don't have anything new from sql to rebuild lists
    When we execute command on clickhouse02
    """
    test -f /var/lib/clickhouse/access/need_rebuild_lists.mark && echo "exists" || echo "does not exist"
    """
    Then we get response
    """
    does not exist
    """
    # check USERS
    When we execute query on clickhouse02
    """
    SHOW USERS;
    """
    Then we get response not contains user01
    # check ROLES
    When we execute query on clickhouse02
    """
    SHOW ROLES;
    """
    Then we get response not contains role01
    # check QUOTAS
    When we execute query on clickhouse02
    """
    SHOW QUOTAS;
    """
    Then we get response not contains quota01
    # check PROFILES
    When we execute query on clickhouse02
    """
    SHOW PROFILES;
    """
    Then we get response not contains profile01
    # check ROW POLICIES
    When we execute query on clickhouse02
    """
    SHOW ROW POLICIES;
    """
    Then we get response not contains policy01

  @require_version_22.3
  Scenario: migrate to replicated
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
    policy01 ON test.table_01
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

  @require_version_22.3
  Scenario: migrate to local
    When we execute command on clickhouse02
    """
    chadmin access-storage --host zookeeper01 migrate-to-local
    """
    # now we have something new from sql files to rebuild lists
    When we execute command on clickhouse02
    """
    test -f /var/lib/clickhouse/access/need_rebuild_lists.mark && echo "exists" || echo "does not exist"
    """
    Then we get response
    """
    exists
    """
    # we have to restart CH to rebuild lists and wait a little bit for that
    When we execute command on clickhouse02
    """
    supervisorctl restart clickhouse-server
    """
    And we sleep for 10 seconds
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
    Then we get response contains
    """
    user01
    """
    # check that we have particular role in CH
    When we execute query on clickhouse02
    """
    SHOW ROLES;
    """
    Then we get response contains
    """
    role01
    """
    # check that we have particular quota in CH
    When we execute query on clickhouse02
    """
    SHOW QUOTAS;
    """
    Then we get response contains
    """
    quota01
    """
    # check that we have particular profile in CH
    When we execute query on clickhouse02
    """
    SHOW PROFILES;
    """
    Then we get response contains
    """
    profile01
    """
    # check that we have particular row policy in CH
    When we execute query on clickhouse02
    """
    SHOW ROW POLICIES;
    """
    Then we get response contains
    """
    policy01
    """
