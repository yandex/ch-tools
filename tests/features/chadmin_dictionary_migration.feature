Feature: chadmin dictionary migration command.

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And we have executed queries on clickhouse01
    """
      CREATE DATABASE IF NOT EXISTS test_db;
      DROP TABLE IF EXISTS test_db.test;
      CREATE TABLE test_db.test(id UInt64, name String, age UInt64) ENGINE=MergeTree() ORDER BY id PARTITION BY id;
      INSERT INTO test_db.test VALUES (1, 'one', 1), (2, 'two', 2), (3, 'three', 3);
    """

  Scenario: Migrate external dictionaries.
    When we execute command on clickhouse01
    """
      echo -e "
          <dictionaries>
            <dictionary>
              <name>test_dict1</name>
              <source>
                <clickhouse>
                  <db>test_db</db>
                  <table>test</table>
                </clickhouse>
              </source>
              <layout><flat/></layout>
              <lifetime><min>0</min><max>100</max></lifetime>
              <structure>
                <id><name>id</name></id>
                <attribute>
                  <name>name</name>
                  <type>String</type>
                </attribute>
              </structure>
            </dictionary>
            
            <dictionary>
              <name>test_dict2</name>
              <source>
                <clickhouse>
                  <db>test_db</db>
                  <table>test</table>
                </clickhouse>
              </source>
              <layout><flat/></layout>
              <lifetime>0</lifetime>
              <structure>
                <id><name>id</name></id>
                <attribute>
                  <name>age</name>
                  <type>UInt64</type>
                </attribute>
              </structure>
            </dictionary>
          </dictionaries>
          " > /etc/clickhouse-server/test_dictionary.xml && \
      supervisorctl restart clickhouse-server
    """
    And we execute command on clickhouse01:
    """
      chadmin dictionary migrate
    """
    Then it completes successfully
    And dictionary "_dictionaries.test_dict1" returns expected values on clickhouse01
      | id           | attribute | expected |
      | toUInt64(1)  | name      | one      |
      | toUInt64(2)  | name      | two      |
      | toUInt64(3)  | name      | three    |
      | toUInt64(4)  | name      |          |
    And dictionary "_dictionaries.test_dict2" returns expected values on clickhouse01
      | id           | attribute | expected |
      | toUInt64(1)  | age       | 1        |
      | toUInt64(2)  | age       | 2        |
      | toUInt64(3)  | age       | 3        |
      | toUInt64(4)  | age       | 0        |