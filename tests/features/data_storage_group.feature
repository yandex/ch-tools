Feature: chadmin data-store commands

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    
  Scenario: Check collect clean orphaned data.
    When we execute command on clickhouse01
    """
    mkdir -p /var/lib/clickhouse/disks/object_storage/store/123/1234678-f435-49dd-9358-fffc12b758b0
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml data-store clean-orphaned-tables --store-path /var/lib/clickhouse/disks/object_storage/store
    """
    Then we get response contains
    """
    - path: /var/lib/clickhouse/disks/object_storage/store/123
      status: not_used
      size: 8.0K
      removed: false
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml data-store clean-orphaned-tables --store-path /var/lib/clickhouse/disks/object_storage/store --remove
    """
    Then we get response contains
    """
    - path: /var/lib/clickhouse/disks/object_storage/store/123
      status: not_used
      size: 8.0K
      removed: true
    """

  Scenario: Check remove orphaned sql objects.
    When we execute queries on clickhouse01
    """
    CREATE DATABASE db1 Engine=Lazy(20);
    CREATE TABLE db1.test (a int) Engine=StripeLog() SETTINGS storage_policy='object_storage';
    """
    When we execute command on clickhouse01
    """
    mkdir -p /var/lib/clickhouse/disks/object_storage/data/db1/test1
    """
    When we execute command on clickhouse01
    """
    mkdir -p /var/lib/clickhouse/disks/object_storage/data/db2/test2
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml data-store cleanup-data-dir --disk object_storage
    """
    Then we get response contains
    """
    - path: /var/lib/clickhouse/disks/object_storage/data/db2
      deleted: 'No'
    - path: /var/lib/clickhouse/disks/object_storage/data/db1/test1
      deleted: 'No'
    """
    When we execute command on clickhouse01
    """
    chadmin --format yaml data-store cleanup-data-dir --disk object_storage --remove
    """
    Then we get response contains
    """
    - path: /var/lib/clickhouse/disks/object_storage/data/db2
      deleted: 'Yes'
    - path: /var/lib/clickhouse/disks/object_storage/data/db1/test1
      deleted: 'Yes'
    """