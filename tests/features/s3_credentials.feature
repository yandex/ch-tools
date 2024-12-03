Feature: ch_s3_credentials tool

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And a working http server

    Scenario Outline: chadmin s3 check work correctly
    When we execute command on clickhouse01
    """
    ch-monitoring s3-credentials-config
    """
    Then we get response
    """
    0;OK
    """
    When we execute command on clickhouse01
    """
    chadmin s3-credentials-config --metadata-address=http_mock01:8080 update --endpoint=storage.com
    """
    And we execute command on clickhouse01
    """
    ch-monitoring s3-credentials-config --present
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
    <clickhouse>
        <s3>
            <cloud_storage>
                <endpoint>storage.com</endpoint>
                <<header>>X-YaCloud-SubjectToken: IAM_TOKEN</<header>>
            </cloud_storage>
        </s3>
    </clickhouse>
    """
    @require_version_24.11
    Examples:
    |header|
    |access_header|
    
    @require_version_less_than_24.11
    Examples:
    | header      |
    |header|
