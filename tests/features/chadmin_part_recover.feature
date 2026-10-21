Feature: Recover data from broken detached object-storage parts

  Background:
    Given default configuration
    And a working s3
    And a working clickhouse on clickhouse01

  @require_version_25.8
  Scenario: Recover a Wide part by inferred and explicit source table
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS recovery_source;
    DROP DATABASE IF EXISTS recovered_by_path;
    DROP DATABASE IF EXISTS recovered_by_name;
    DROP DATABASE IF EXISTS recovered_by_table;

    CREATE DATABASE recovery_source;
    CREATE TABLE recovery_source.source
    (
        id UInt64,
        data String,
        lost String
    )
    ENGINE = MergeTree
    ORDER BY tuple()
    SETTINGS
        storage_policy = 'object_storage',
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0;

    INSERT INTO recovery_source.source VALUES
        (1, 'one', 'lost-one'),
        (2, 'two', 'lost-two'),
        (3, 'three', 'lost-three');

    ALTER TABLE recovery_source.source DETACH PARTITION tuple();
    """
    And we remove S3 blobs for file lost.bin from detached part recovery_source.source on clickhouse01

    When we execute command on clickhouse01
    """
    PART_PATH=$(clickhouse client --query "
        SELECT path
        FROM system.detached_parts
        WHERE database = 'recovery_source' AND table = 'source'
    ")
    chadmin --format yaml part recover --path "$PART_PATH" --target-table recovered_by_path.data
    """
    Then we get response contains
    """
    source_column: lost
    """
    And we get response contains
    """
    status: lost
    """

    When we execute query on clickhouse01
    """
    SELECT name
    FROM system.columns
    WHERE database = 'recovered_by_path' AND table = 'data'
    ORDER BY position
    """
    Then we get response
    """
    id
    data
    """

    When we execute query on clickhouse01
    """
    SELECT id, data
    FROM recovered_by_path.data
    ORDER BY id
    """
    Then we get query response
    """
    1\tone
    2\ttwo
    3\tthree
    """

    When we execute command on clickhouse01
    """
    PART_NAME=$(clickhouse client --query "
        SELECT name
        FROM system.detached_parts
        WHERE database = 'recovery_source' AND table = 'source'
    ")
    chadmin part recover --database recovery_source --table source --name "$PART_NAME" --target-table recovered_by_name.data
    """

    When we execute query on clickhouse01
    """
    SELECT id, data
    FROM recovered_by_name.data
    ORDER BY id
    """
    Then we get query response
    """
    1\tone
    2\ttwo
    3\tthree
    """

    When we execute command on clickhouse01
    """
    PART_PATH=$(clickhouse client --query "
        SELECT path
        FROM system.detached_parts
        WHERE database = 'recovery_source' AND table = 'source'
    ")
    DISK_PATH=$(clickhouse client --query "
        SELECT path FROM system.disks WHERE name = 'object_storage'
    ")
    RECOVERY_PATH="${DISK_PATH%/}/recovery-input/broken_part"
    mkdir -p "${RECOVERY_PATH%/*}"
    mv "$PART_PATH" "$RECOVERY_PATH"

    chadmin part recover --path "$RECOVERY_PATH" --database recovery_source --table source --target-table recovered_by_table.data
    """

    When we execute query on clickhouse01
    """
    SELECT id, data
    FROM recovered_by_table.data
    ORDER BY id
    """
    Then we get query response
    """
    1\tone
    2\ttwo
    3\tthree
    """

    When we execute command on clickhouse01
    """
    DISK_PATH=$(clickhouse client --query "
        SELECT path FROM system.disks WHERE name = 'object_storage'
    ")
    RECOVERY_PATH="${DISK_PATH%/}/recovery-input/broken_part"
    chadmin --format yaml part recover --dry-run --path "$RECOVERY_PATH" --database recovery_source --table source
    """
    Then we get response contains
    """
    target_table: recovery_source.source_recovered_broken_part
    """
    And we get response contains
    """
    status: recoverable
    """

    When we execute query on clickhouse01
    """
    SELECT count()
    FROM system.tables
    WHERE database = 'recovery_source'
      AND name = 'source_recovered_broken_part'
    """
    Then we get query response
    """
    0
    """

    When we execute command on clickhouse01
    """
    DISK_PATH=$(clickhouse client --query "
        SELECT path FROM system.disks WHERE name = 'object_storage'
    ")
    RECOVERY_PATH="${DISK_PATH%/}/recovery-input/broken_part"
    chadmin --format yaml part recover --path "$RECOVERY_PATH" --database recovery_source --table source
    """
    Then we get response contains
    """
    target_table: recovery_source.source_recovered_broken_part
    """

    When we execute query on clickhouse01
    """
    SELECT id, data
    FROM recovery_source.source_recovered_broken_part
    ORDER BY id
    """
    Then we get query response
    """
    1\tone
    2\ttwo
    3\tthree
    """

  @require_version_25.8
  Scenario: Recover a Compact part with broken regenerable metadata
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS compact_recovery_source;
    DROP DATABASE IF EXISTS recovered_compact;

    CREATE DATABASE compact_recovery_source;
    CREATE TABLE compact_recovery_source.source
    (
        id UInt64,
        value String
    )
    ENGINE = MergeTree
    ORDER BY tuple()
    SETTINGS
        storage_policy = 'object_storage',
        min_bytes_for_wide_part = 1000000000,
        min_rows_for_wide_part = 1000000000;

    INSERT INTO compact_recovery_source.source VALUES
        (1, 'one'),
        (2, 'two');

    ALTER TABLE compact_recovery_source.source DETACH PARTITION tuple();
    """
    And we remove S3 blobs for file checksums.txt from detached part compact_recovery_source.source on clickhouse01
    And we remove S3 blobs for file default_compression_codec.txt from detached part compact_recovery_source.source on clickhouse01

    When we execute command on clickhouse01
    """
    PART_NAME=$(clickhouse client --query "
        SELECT name FROM system.detached_parts
        WHERE database = 'compact_recovery_source' AND table = 'source'
    ")
    chadmin part recover --database compact_recovery_source --table source --name "$PART_NAME" --target-table recovered_compact.data
    """

    When we execute query on clickhouse01
    """
    SELECT id, value FROM recovered_compact.data ORDER BY id
    """
    Then we get query response
    """
    1\tone
    2\ttwo
    """

  @require_version_25.8
  Scenario: Restore a known-empty S3 object before recovering a Wide part
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS empty_object_source;
    DROP DATABASE IF EXISTS recovered_empty_object;

    CREATE DATABASE empty_object_source;
    CREATE TABLE empty_object_source.source
    (
        id UInt64,
        empty_values Array(UInt64)
    )
    ENGINE = MergeTree
    ORDER BY tuple()
    SETTINGS
        storage_policy = 'object_storage',
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0;

    INSERT INTO empty_object_source.source VALUES
        (1, []),
        (2, []);

    ALTER TABLE empty_object_source.source DETACH PARTITION tuple();
    """
    And we make file empty_values.bin reference a missing empty S3 object in detached part empty_object_source.source on clickhouse01

    When we execute command on clickhouse01
    """
    PART_NAME=$(clickhouse client --query "
        SELECT name FROM system.detached_parts
        WHERE database = 'empty_object_source' AND table = 'source'
    ")
    chadmin part recover --database empty_object_source --table source --name "$PART_NAME" --target-table recovered_empty_object.data
    """
    Then we get response contains
    """
    Restored 1 missing empty S3 object(s)
    """

    When we execute query on clickhouse01
    """
    SELECT id, empty_values
    FROM recovered_empty_object.data
    ORDER BY id
    """
    Then we get query response
    """
    1\t[]
    2\t[]
    """

  @require_version_25.8
  Scenario Outline: Reject a detached part without required <filename>
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS missing_metadata_source;
    DROP DATABASE IF EXISTS recovered_missing_metadata;

    CREATE DATABASE missing_metadata_source;
    CREATE TABLE missing_metadata_source.source
    (
        id UInt64,
        lost String
    )
    ENGINE = MergeTree
    ORDER BY tuple()
    SETTINGS
        storage_policy = 'object_storage',
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0;

    INSERT INTO missing_metadata_source.source VALUES (1, 'lost');
    ALTER TABLE missing_metadata_source.source DETACH PARTITION tuple();
    """
    And we remove S3 blobs for file lost.bin from detached part missing_metadata_source.source on clickhouse01
    And we remove S3 blobs for file <filename> from detached part missing_metadata_source.source on clickhouse01

    When we try to execute command on clickhouse01
    """
    PART_NAME=$(clickhouse client --query "
        SELECT name FROM system.detached_parts
        WHERE database = 'missing_metadata_source' AND table = 'source'
    ")
    chadmin part recover --database missing_metadata_source --table source --name "$PART_NAME" --target-table recovered_missing_metadata.data
    """
    Then it fails with response contains
    """
    <error>
    """

    Examples:
    | filename    | error                                             |
    | columns.txt | the original part schema cannot be inferred safely |
    | count.txt   | the physical row count is unavailable after detach  |

  @require_version_25.8
  Scenario: Recover a part after count.txt was restored before detach
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS pre_repaired_source;
    DROP DATABASE IF EXISTS recovered_pre_repaired;

    CREATE DATABASE pre_repaired_source;
    CREATE TABLE pre_repaired_source.source
    (
        id UInt64,
        keep String,
        lost String
    )
    ENGINE = MergeTree
    ORDER BY tuple()
    SETTINGS
        storage_policy = 'object_storage',
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0;

    INSERT INTO pre_repaired_source.source VALUES
        (1, 'one', 'lost-one'),
        (2, 'two', 'lost-two');
    """
    And we remove s3 object for active part file count.txt from table pre_repaired_source.source on clickhouse01
    And we remove s3 object for active part file lost.bin from table pre_repaired_source.source on clickhouse01

    When we execute command on clickhouse01
    """
    chadmin --format yaml data-store detect-broken-partitions --restore-recoverable --detach --max-broken-parts-percentage 100
    """
    Then we get response contains
    """
        - name: count.txt
          check: missing
          recoverable: true
          action: restored
    """

    When we execute command on clickhouse01
    """
    PART_NAME=$(clickhouse client --query "
        SELECT name FROM system.detached_parts
        WHERE database = 'pre_repaired_source' AND table = 'source'
    ")
    chadmin part recover --database pre_repaired_source --table source --name "$PART_NAME" --target-table recovered_pre_repaired.data
    """

    When we execute query on clickhouse01
    """
    SELECT id, keep
    FROM recovered_pre_repaired.data
    ORDER BY id
    """
    Then we get query response
    """
    1\tone
    2\ttwo
    """

  @require_version_25.8
  Scenario: Recover a Wide part with Nested columns
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS nested_recovery_source;
    DROP DATABASE IF EXISTS recovered_nested;

    CREATE DATABASE nested_recovery_source;
    CREATE TABLE nested_recovery_source.source
    (
        id UInt64,
        items Nested(name String, value UInt64),
        lost String
    )
    ENGINE = MergeTree
    ORDER BY tuple()
    SETTINGS
        storage_policy = 'object_storage',
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0;

    INSERT INTO nested_recovery_source.source VALUES
        (1, ['one', 'two'], [10, 20], 'lost-one'),
        (2, ['three'], [30], 'lost-two');

    ALTER TABLE nested_recovery_source.source DETACH PARTITION tuple();
    """
    And we remove S3 blobs for file lost.bin from detached part nested_recovery_source.source on clickhouse01

    When we execute command on clickhouse01
    """
    PART_NAME=$(clickhouse client --query "
        SELECT name FROM system.detached_parts
        WHERE database = 'nested_recovery_source' AND table = 'source'
    ")
    chadmin part recover --database nested_recovery_source --table source --name "$PART_NAME" --target-table recovered_nested.data
    """

    When we execute query on clickhouse01
    """
    SELECT
        id,
        arrayStringConcat(items.name, ','),
        arrayStringConcat(arrayMap(item -> toString(item), items.value), ',')
    FROM recovered_nested.data
    ORDER BY id
    """
    Then we get query response
    """
    1\tone,two\t10,20
    2\tthree\t30
    """

  @require_version_25.8
  Scenario: Recover a Wide part with a lightweight-delete mask
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS lightweight_delete_source;
    DROP DATABASE IF EXISTS recovered_lightweight_delete;

    CREATE DATABASE lightweight_delete_source;
    CREATE TABLE lightweight_delete_source.source
    (
        id UInt64,
        value String,
        lost String
    )
    ENGINE = MergeTree
    ORDER BY tuple()
    SETTINGS
        storage_policy = 'object_storage',
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0;

    INSERT INTO lightweight_delete_source.source VALUES
        (1, 'one', 'lost-one'),
        (2, 'two', 'lost-two'),
        (3, 'three', 'lost-three');

    SET lightweight_deletes_sync = 1;
    DELETE FROM lightweight_delete_source.source WHERE id = 2;
    ALTER TABLE lightweight_delete_source.source DETACH PARTITION tuple();
    """
    And we remove S3 blobs for file lost.bin from detached part lightweight_delete_source.source on clickhouse01

    When we execute command on clickhouse01
    """
    PART_NAME=$(clickhouse client --query "
        SELECT name FROM system.detached_parts
        WHERE database = 'lightweight_delete_source' AND table = 'source'
    ")
    chadmin part recover --database lightweight_delete_source --table source --name "$PART_NAME" --target-table recovered_lightweight_delete.data
    """

    When we execute query on clickhouse01
    """
    SELECT id, value, _recovery_row_exists
    FROM recovered_lightweight_delete.data
    ORDER BY id
    """
    Then we get query response
    """
    1\tone\t1
    2\ttwo\t0
    3\tthree\t1
    """

  @require_version_less_than_25.8
  Scenario: Reject part recovery on an unsupported ClickHouse version
    When we try to execute command on clickhouse01
    """
    chadmin part recover --database default --table source --name all_1_1_0 --target-table recovered.data
    """
    Then it fails with response contains
    """
    Part recovery requires ClickHouse version 25.8 or above
    """
