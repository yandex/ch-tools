Feature: chadmin performance diagnostics.

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario: Sanity checks:
    When we execute command on clickhouse01
    """
    chadmin perfomance-dianostics collect-flamegraph-by-interval --trace-type CPU
    """
    And we execute command on clickhouse01
    """
    chadmin perfomance-dianostics setup-ch-settings-for-flamegraph --trace-type MemorySample
    """
    And we execute command on clickhouse01
    """
    clickhouse client --query-id 123 --query ' SELECT count(*) FROM numbers(4000000) AS l LEFT JOIN (select rand32()%1000000 AS number FROM numbers(40000000)) AS r ON l.number=r.number SETTINGS use_query_cache=0;'
    """
    And we execute command on clickhouse01
    """
    chadmin perfomance-dianostics collect-flame-graph-by-query-id --query-id 123 --trace-type MemorySample
    """
    And we execute command on clickhouse01
    """
    chadmin perfomance-dianostics remove-ch-settings-for-flamegraph --trace-type MemorySample
    """
    Then it completes successfully
