Feature: chadmin performance diagnostics.

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  @require_version_23.8
  Scenario: Sanity checks:
    When we execute command on clickhouse01
    """
    chadmin flamegraph collect-by-interval --trace-type CPU
    """
    And we execute command on clickhouse01
    """
    chadmin flamegraph setup --trace-type MemorySample
    """
    And we execute command on clickhouse01
    """
    clickhouse client --query-id 123 --query ' SELECT count(*) FROM numbers(4000000) AS l LEFT JOIN (select rand32()%1000000 AS number FROM numbers(40000000)) AS r ON l.number=r.number SETTINGS use_query_cache=0;'
    """
    And we execute command on clickhouse01
    """
    chadmin flamegraph collect-by-query --query-id 123 --trace-type MemorySample
    """
    And we execute command on clickhouse01
    """
    chadmin flamegraph cleanup --trace-type MemorySample
    """
    And we execute command on clickhouse01
    """
    chadmin flamegraph setup --trace-type Real
    """
    And we execute command on clickhouse01
    """
    clickhouse client --query-id 1234 --query ' SELECT count(*) FROM numbers(4000000) AS l LEFT JOIN (select rand32()%1000000 AS number FROM numbers(40000000)) AS r ON l.number=r.number SETTINGS use_query_cache=0;'
    """
    And we execute command on clickhouse01
    """
    chadmin flamegraph collect-by-query --query-id 1234 --trace-type Real
    """
    And we execute command on clickhouse01
    """
    chadmin flamegraph cleanup --trace-type MemorySample
    """
    Then it completes successfully
