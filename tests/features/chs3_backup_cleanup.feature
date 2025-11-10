Feature: Cleanup of orphaned S3 disk backups

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01

  Scenario: Cleanup of orphaned S3 disk backups
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE db1;

    CREATE TABLE db1.table1 (n Int32)
    ENGINE = ReplicatedMergeTree('/tables/db1_table1', '{replica}')
    ORDER BY n PARTITION BY n SETTINGS storage_policy = 'hybrid_storage';

    INSERT INTO db1.table1 (n) VALUES (1) (11);
    ALTER TABLE db1.table1 MOVE PARTITION ID '11' TO DISK 'object_storage'
    """
    And we have executed command on clickhouse01
    """
    ch-backup backup --name backup1_partially_deleted
    """
    And we have executed queries on clickhouse01
    """
    INSERT INTO db1.table1 (n) VALUES (2) (12);
    ALTER TABLE db1.table1 MOVE PARTITION ID '12' TO DISK 'object_storage'
    """
    And we have executed command on clickhouse01
    """
    ch-backup backup --name backup2
    """
    And we have executed command on clickhouse01
    """
    ch-backup delete backup1_partially_deleted
    """
    And we have executed queries on clickhouse01
    """
    ALTER TABLE db1.table1 FREEZE WITH NAME 'backup1_partially_deleted';
    ALTER TABLE db1.table1 FREEZE WITH NAME 'backup3_deleted';
    """

    When we execute command on clickhouse01
    """
    ch-monitoring orphaned-backups
    """
    Then we get response
    """
    1;There are 2 orphaned S3 backups: backup1_partially_deleted, backup3_deleted
    """

    When we execute command on clickhouse01
    """
    chadmin chs3-backup cleanup
    """
    And we execute command on clickhouse01
    """
    ch-monitoring orphaned-backups
    """
    Then we get response
    """
    0;OK
    """
    When we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/disks/object_storage/shadow
    """
    Then we get response
    """
    backup2
    """
