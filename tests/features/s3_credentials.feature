Feature: ch_s3_credentials tool

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And a working http server

  Scenario: s3 check work correctly
    When we execute command on clickhouse01
    """
    ch-s3-credentials check
    """
    Then we get response
    """
    0;OK
    """
    When we execute command on clickhouse01
    """
    ch-s3-credentials --metadata-address=http_mock01:8080 update --endpoint=storage.com
    """
    And we execute command on clickhouse01
    """
    ch-s3-credentials check --present
    """
    Then we get response
    """
    0;OK
    """
    When we execute command on clickhouse01
    """
    cat /etc/clickhouse-server/config.d/s3_credentials.xml
    """
    Then we get response
    """
    <?xml version="1.0" encoding="utf-8"?>
    <yandex>
        <s3>
            <cloud_storage>
                <endpoint>storage.com</endpoint>
                <header>X-YaCloud-SubjectToken: IAM_TOKEN</header>
            </cloud_storage>
        </s3>
    </yandex>
    """