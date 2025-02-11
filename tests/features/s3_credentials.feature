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
    ch-monitoring --setting cloud.metadata_service_endpoint http://http_mock01:8080 s3-credentials-config --missing
    """
    Then we get response
    """
    0;OK
    """
    When we execute command on clickhouse01
    """
    chadmin --setting cloud.metadata_service_endpoint http://http_mock01:8080 s3-credentials-config update --endpoint=storage.com
    """
    And we execute command on clickhouse01
    """
    ch-monitoring --setting cloud.metadata_service_endpoint http://http_mock01:8080 s3-credentials-config --present
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
                <<header_tag_name>>X-YaCloud-SubjectToken: IAM_TOKEN</<header_tag_name>>
            </cloud_storage>
        </s3>
    </clickhouse>
    """
    @require_version_24.11
    Examples:
    | header_tag_name |
    | access_header   |
    
    @require_version_less_than_24.11
    Examples:
    | header_tag_name |
    | header          |

    Scenario: Offline token update.
    Given installed clickhouse-tools config with version on clickhouse01
    When we execute command on clickhouse01
    """
    ch-monitoring --setting cloud.metadata_service_endpoint http://http_mock01:8080 s3-credentials-config --missing
    """
    When we execute command on clickhouse01
    """
    supervisorctl stop clickhouse-server
    """
    When we execute command on clickhouse01
    """
    chadmin --setting cloud.metadata_service_endpoint http://http_mock01:8080 s3-credentials-config update --endpoint=storage.com
    """
    And we execute command on clickhouse01
    """
    ch-monitoring --setting cloud.metadata_service_endpoint http://http_mock01:8080 s3-credentials-config --present
    """
    Then we get response
    """
    0;OK
    """
