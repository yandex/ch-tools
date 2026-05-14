Feature: chadmin part recover-broken

  Background:
    Given default configuration
    And a working S3
    And a working zookeeper
    And a working clickhouse on clickhouse01

  @require_version_23.3
  Scenario: Recover Wide part with one missing data column blob
    Given we have executed queries on clickhouse01
    """
    DROP TABLE IF EXISTS default.test_recovery;
    CREATE TABLE default.test_recovery (
        id     UInt64,
        ts     DateTime,
        value  String,
        extra  UInt32
    )
    ENGINE = MergeTree
    ORDER BY (id, ts)
    SETTINGS storage_policy = 'object_storage', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    INSERT INTO default.test_recovery
    SELECT
        number AS id,
        now() + number AS ts,
        concat('val_', toString(number)) AS value,
        toUInt32(number * 2) AS extra
    FROM numbers(1000);
    OPTIMIZE TABLE default.test_recovery FINAL;
    """
    Given we corrupt column value blobs of part of table default.test_recovery on clickhouse01
    When we try to execute command on clickhouse01
    """
    bash -c 'chadmin part recover-broken --part-path "$(cat /tmp/broken_part_path)" --output /tmp/recovered.tsv --report /tmp/report.json 2>&1; echo "EXIT_CODE:$?"'
    """
    Then we get response contains
    """
    EXIT_CODE:0
    """
    When we try to execute command on clickhouse01
    """
    bash -c 'wc -l < /tmp/recovered.tsv | tr -d " \n"'
    """
    Then we get response
    """
    1000
    """
    When we try to execute command on clickhouse01
    """
    bash -c 'python3 -c "import json; d=json.load(open(\"/tmp/report.json\")); print(\"value\" in d[\"broken_columns\"])"'
    """
    Then we get response
    """
    True
    """
    Given we have executed queries on clickhouse01
    """
    DROP TABLE IF EXISTS default.test_recovery;
    """

  @require_version_23.3
  Scenario: Dry-run mode does not create output file
    Given we have executed queries on clickhouse01
    """
    DROP TABLE IF EXISTS default.test_recovery_dry;
    CREATE TABLE default.test_recovery_dry (
        id    UInt64,
        value String
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS storage_policy = 'object_storage', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    INSERT INTO default.test_recovery_dry
    SELECT number, concat('v', toString(number)) FROM numbers(100);
    OPTIMIZE TABLE default.test_recovery_dry FINAL;
    """
    Given we corrupt column value blobs of part of table default.test_recovery_dry on clickhouse01
    When we try to execute command on clickhouse01
    """
    bash -c 'rm -f /tmp/dry_run_out.tsv && chadmin part recover-broken --part-path "$(cat /tmp/broken_part_path)" --output /tmp/dry_run_out.tsv --dry-run'
    """
    Then it completes successfully
    When we try to execute command on clickhouse01
    """
    bash -c 'test ! -f /tmp/dry_run_out.tsv && echo NOT_EXISTS || echo EXISTS'
    """
    Then we get response
    """
    NOT_EXISTS
    """
    Given we have executed queries on clickhouse01
    """
    DROP TABLE IF EXISTS default.test_recovery_dry;
    """

  @require_version_23.3
  Scenario: Missing columns.txt is reconstructed from system.columns
    Given we have executed queries on clickhouse01
    """
    DROP TABLE IF EXISTS default.test_recovery_critical;
    CREATE TABLE default.test_recovery_critical (
        id    UInt64,
        value String
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS storage_policy = 'object_storage', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    INSERT INTO default.test_recovery_critical
    SELECT number, concat('v', toString(number)) FROM numbers(50);
    OPTIMIZE TABLE default.test_recovery_critical FINAL;
    """
    Given we corrupt column value blobs of part of table default.test_recovery_critical on clickhouse01
    When we execute command on clickhouse01
    """
    bash -c 'rm -f "$(cat /tmp/broken_part_path)/columns.txt"'
    """
    When we try to execute command on clickhouse01
    """
    bash -c 'chadmin part recover-broken --part-path "$(cat /tmp/broken_part_path)" --output /tmp/critical_out.tsv; echo "EXIT:$?"'
    """
    Then we get response contains
    """
    EXIT:0
    """
    And we get response contains
    """
    Reconstructed columns.txt for part
    """
    And we get response contains
    """
    system.columns
    """
    Given we have executed queries on clickhouse01
    """
    DROP TABLE IF EXISTS default.test_recovery_critical;
    """

  @require_version_23.3
  Scenario: All blobs healthy — full recovery without NULL columns
    Given we have executed queries on clickhouse01
    """
    DROP TABLE IF EXISTS default.test_recovery_full;
    CREATE TABLE default.test_recovery_full (
        id    UInt64,
        value String
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS storage_policy = 'object_storage', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    INSERT INTO default.test_recovery_full
    SELECT number, concat('v', toString(number)) FROM numbers(200);
    OPTIMIZE TABLE default.test_recovery_full FINAL;
    """
    Given we copy part of table default.test_recovery_full to detached on clickhouse01
    When we execute command on clickhouse01
    """
    bash -c 'chadmin part recover-broken --part-path "$(cat /tmp/broken_part_path)" --output /tmp/full_recovered.tsv --report /tmp/full_report.json'
    """
    Then it completes successfully
    When we try to execute command on clickhouse01
    """
    bash -c 'wc -l < /tmp/full_recovered.tsv | tr -d " \n"'
    """
    Then we get response
    """
    200
    """
    When we try to execute command on clickhouse01
    """
    bash -c 'python3 -c "import json; d=json.load(open(\"/tmp/full_report.json\")); print(d[\"broken_columns\"])"'
    """
    Then we get response
    """
    []
    """
    Given we have executed queries on clickhouse01
    """
    DROP TABLE IF EXISTS default.test_recovery_full;
    """

  @require_version_23.3
  Scenario: Compact part with healthy data.bin — full recovery
    Given we have executed queries on clickhouse01
    """
    DROP TABLE IF EXISTS default.test_compact_recovery;
    CREATE TABLE default.test_compact_recovery (
        id    UInt64,
        value String
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS storage_policy = 'object_storage', min_bytes_for_wide_part = 9223372036854775807, min_rows_for_wide_part = 9223372036854775807;
    INSERT INTO default.test_compact_recovery
    SELECT number, concat('v', toString(number)) FROM numbers(100);
    OPTIMIZE TABLE default.test_compact_recovery FINAL;
    """
    Given we copy part of table default.test_compact_recovery to detached on clickhouse01
    When we execute command on clickhouse01
    """
    bash -c 'chadmin part recover-broken --part-path "$(cat /tmp/broken_part_path)" --output /tmp/compact_recovered.tsv --report /tmp/compact_report.json'
    """
    Then it completes successfully
    When we try to execute command on clickhouse01
    """
    bash -c 'wc -l < /tmp/compact_recovered.tsv | tr -d " \n"'
    """
    Then we get response
    """
    100
    """
    When we try to execute command on clickhouse01
    """
    bash -c 'python3 -c "import json; d=json.load(open(\"/tmp/compact_report.json\")); print(len(d[\"broken_columns\"]))"'
    """
    Then we get response
    """
    0
    """
    Given we have executed queries on clickhouse01
    """
    DROP TABLE IF EXISTS default.test_compact_recovery;
    """

  @require_version_23.3
  Scenario: Compact part with missing data.bin blobs — exit code 2
    Given we have executed queries on clickhouse01
    """
    DROP TABLE IF EXISTS default.test_compact_critical;
    CREATE TABLE default.test_compact_critical (
        id    UInt64,
        value String
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS storage_policy = 'object_storage', min_bytes_for_wide_part = 9223372036854775807, min_rows_for_wide_part = 9223372036854775807;
    INSERT INTO default.test_compact_critical
    SELECT number, concat('v', toString(number)) FROM numbers(50);
    OPTIMIZE TABLE default.test_compact_critical FINAL;
    """
    Given we corrupt data.bin blobs of part of table default.test_compact_critical on clickhouse01
    When we try to execute command on clickhouse01
    """
    bash -c 'chadmin part recover-broken --part-path "$(cat /tmp/broken_part_path)" --output /tmp/compact_critical.tsv; echo "EXIT:$?"'
    """
    Then we get response contains
    """
    EXIT:2
    """
    Given we have executed queries on clickhouse01
    """
    DROP TABLE IF EXISTS default.test_compact_critical;
    """
