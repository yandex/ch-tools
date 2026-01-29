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

  Scenario: Migrate external dictionaries without removal.
    When we execute command on clickhouse01:
    """
      echo -e "
          <dictionary>
            <name>prod_dict</name>
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
                <null_value/>
              </attribute>
            </structure>
          </dictionary>
          " > /etc/clickhouse-server/prod_dictionary.xml && \
      echo -e "
          <clickhouse>
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
                  <null_value/>
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
                  <null_value/>
                </attribute>
              </structure>
            </dictionary>
          </clickhouse>
          " > /etc/clickhouse-server/test_dictionary.xml && \
      supervisorctl restart clickhouse-server
    """
    And we execute command on clickhouse01:
    """
      chadmin dictionary migrate
    """
    Then it completes successfully
    And dictionary "_dictionaries.prod_dict" returns expected values on clickhouse01
      | id           | attribute | expected |
      | toUInt64(1)  | name      | one      |
      | toUInt64(2)  | name      | two      |
      | toUInt64(3)  | name      | three    |
      | toUInt64(4)  | name      |          |
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
    
    When we execute command on clickhouse01:
    """
      test -f /etc/clickhouse-server/prod_dictionary.xml
    """
    And we execute command on clickhouse01:
    """
      test -f /etc/clickhouse-server/prod_dictionary.xml
    """
    Then it completes successfully
  
  Scenario: Migrate external dictionaries with removal.
    When we execute command on clickhouse01:
    """
      echo -e "
          <dictionary>
            <name>prod_dict</name>
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
                <null_value/>
              </attribute>
            </structure>
          </dictionary>
          " > /etc/clickhouse-server/prod_dictionary.xml && \
      echo -e "
          <clickhouse>
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
                  <null_value/>
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
                  <null_value/>
                </attribute>
              </structure>
            </dictionary>
          </clickhouse>
          " > /etc/clickhouse-server/test_dictionary.xml && \
      supervisorctl restart clickhouse-server
    """
    And we execute command on clickhouse01:
    """
      chadmin dictionary migrate --remove
    """
    Then it completes successfully
    And dictionary "_dictionaries.prod_dict" returns expected values on clickhouse01
      | id           | attribute | expected |
      | toUInt64(1)  | name      | one      |
      | toUInt64(2)  | name      | two      |
      | toUInt64(3)  | name      | three    |
      | toUInt64(4)  | name      |          |
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
    
    When we execute command on clickhouse01:
    """
      test ! -f /etc/clickhouse-server/prod_dictionary.xml
    """
    And we execute command on clickhouse01:
    """
      test ! -f /etc/clickhouse-server/test_dictionary.xml
    """
    Then it completes successfully

  Scenario: Migrate with --include filter and removal.
    When we execute command on clickhouse01:
    """
      echo -e "
          <dictionary>
            <name>prod_dict</name>
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
                <null_value/>
              </attribute>
            </structure>
          </dictionary>
          " > /etc/clickhouse-server/prod_dictionary.xml && \
      echo -e "
          <clickhouse>
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
                  <null_value/>
                </attribute>
              </structure>
            </dictionary>
          </clickhouse>
          " > /etc/clickhouse-server/test_dictionary.xml && \
      supervisorctl restart clickhouse-server
    """
    And we execute command on clickhouse01:
    """
      chadmin dictionary migrate --include "prod_*" --remove
    """
    Then it completes successfully
    And dictionary "_dictionaries.prod_dict" returns expected values on clickhouse01
      | id           | attribute | expected |
      | toUInt64(1)  | name      | one      |
      | toUInt64(2)  | name      | two      |
      | toUInt64(3)  | name      | three    |
      | toUInt64(4)  | name      |          |

    When we try to execute command on clickhouse01:
    """
      clickhouse-client -q "SELECT dictGet('_dictionaries.test_dict1', 'name', toUInt64(1))"
    """
    Then it fails

    When we execute command on clickhouse01:
    """
      test ! -f /etc/clickhouse-server/prod_dictionary.xml
    """
    And we execute command on clickhouse01:
    """
      test -f /etc/clickhouse-server/test_dictionary.xml
    """
    Then it completes successfully
  
  Scenario: Migrate with --exclude filter and removal.
    When we execute command on clickhouse01:
    """
      echo -e "
          <dictionary>
            <name>prod_dict</name>
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
                <null_value/>
              </attribute>
            </structure>
          </dictionary>
          " > /etc/clickhouse-server/prod_dictionary.xml && \
      echo -e "
          <clickhouse>
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
                  <null_value/>
                </attribute>
              </structure>
            </dictionary>
          </clickhouse>
          " > /etc/clickhouse-server/test_dictionary.xml && \
      supervisorctl restart clickhouse-server
    """
    And we execute command on clickhouse01:
    """
      chadmin dictionary migrate --exclude "prod_*" --remove
    """
    Then it completes successfully
    And dictionary "_dictionaries.test_dict1" returns expected values on clickhouse01
      | id           | attribute | expected |
      | toUInt64(1)  | name      | one      |
      | toUInt64(2)  | name      | two      |
      | toUInt64(3)  | name      | three    |
      | toUInt64(4)  | name      |          |
    
    When we try to execute command on clickhouse01:
    """
      clickhouse-client -q "SELECT dictGet('_dictionaries.prod_dict'), 'name', toUInt64(1)"
    """
    Then it fails

    When we execute command on clickhouse01:
    """
      test -f /etc/clickhouse-server/prod_dictionary.xml
    """
    And we execute command on clickhouse01:
    """
      test ! -f /etc/clickhouse-server/test_dictionary.xml
    """
    Then it completes successfully

  Scenario: Migrate to custom database without removal.
    When we execute command on clickhouse01:
    """
      echo -e "
          <dictionary>
            <name>prod_dict</name>
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
                <null_value/>
              </attribute>
            </structure>
          </dictionary>
          " > /etc/clickhouse-server/prod_dictionary.xml && \
      echo -e "
          <clickhouse>
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
                  <null_value/>
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
                  <null_value/>
                </attribute>
              </structure>
            </dictionary>
          </clickhouse>
          " > /etc/clickhouse-server/test_dictionary.xml && \
      supervisorctl restart clickhouse-server
    """
    And we execute command on clickhouse01:
    """
      chadmin dictionary migrate --database prod_db
    """
    Then it completes successfully
    And dictionary "prod_db.prod_dict" returns expected values on clickhouse01
      | id           | attribute | expected |
      | toUInt64(1)  | name      | one      |
      | toUInt64(2)  | name      | two      |
      | toUInt64(3)  | name      | three    |
      | toUInt64(4)  | name      |          |
    And dictionary "prod_db.test_dict1" returns expected values on clickhouse01
      | id           | attribute | expected |
      | toUInt64(1)  | name      | one      |
      | toUInt64(2)  | name      | two      |
      | toUInt64(3)  | name      | three    |
      | toUInt64(4)  | name      |          |
    And dictionary "prod_db.test_dict2" returns expected values on clickhouse01
      | id           | attribute | expected |
      | toUInt64(1)  | age       | 1        |
      | toUInt64(2)  | age       | 2        |
      | toUInt64(3)  | age       | 3        |
      | toUInt64(4)  | age       | 0        |
    
    When we execute command on clickhouse01:
    """
      test -f /etc/clickhouse-server/prod_dictionary.xml
    """
    And we execute command on clickhouse01:
    """
      test -f /etc/clickhouse-server/test_dictionary.xml
    """
    Then it completes successfully
