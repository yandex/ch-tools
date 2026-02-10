"""
Unit tests for table schema diff functionality.
"""

from ch_tools.chadmin.internal.table_schema_diff import normalize_schema


def test_normalize_schema_basic() -> None:
    """Test basic schema normalization."""
    schema = """CREATE TABLE test (
    id UInt64,
    name String
) ENGINE = MergeTree()
ORDER BY id"""

    lines = normalize_schema(schema, remove_replicated=False)

    assert len(lines) > 0
    assert lines[0] == "CREATE TABLE test ("


def test_normalize_schema_remove_replicated_params() -> None:
    """Test removing ReplicatedMergeTree parameters."""
    schema = """CREATE TABLE test (
    id UInt64
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test', '{replica}')
ORDER BY id"""

    lines = normalize_schema(schema, remove_replicated=True)

    # Check that ReplicatedMergeTree parameters are removed
    engine_line = [line for line in lines if "ENGINE" in line][0]
    assert "ReplicatedMergeTree" in engine_line
    assert "'/clickhouse/tables/" not in engine_line
    assert "'{replica}'" not in engine_line


def test_normalize_schema_ignore_uuid() -> None:
    """Test ignoring UUID differences."""
    schema1 = """CREATE TABLE test UUID '12345678-1234-1234-1234-123456789012' (
    id UInt64
) ENGINE = MergeTree()
ORDER BY id"""

    schema2 = """CREATE TABLE test UUID 'abcdefab-abcd-abcd-abcd-abcdefabcdef' (
    id UInt64
) ENGINE = MergeTree()
ORDER BY id"""

    lines1 = normalize_schema(schema1, remove_replicated=False, ignore_uuid=True)
    lines2 = normalize_schema(schema2, remove_replicated=False, ignore_uuid=True)

    # Both should have the same UUID placeholder
    uuid_line1 = [line for line in lines1 if "UUID" in line][0]
    uuid_line2 = [line for line in lines2 if "UUID" in line][0]

    assert uuid_line1 == uuid_line2
    assert "<ignored>" in uuid_line1


def test_normalize_schema_ignore_engine() -> None:
    """Test ignoring engine differences."""
    schema1 = """CREATE TABLE test (
    id UInt64
) ENGINE = MergeTree()
ORDER BY id"""

    schema2 = """CREATE TABLE test (
    id UInt64
) ENGINE = ReplacingMergeTree()
ORDER BY id"""

    lines1 = normalize_schema(schema1, remove_replicated=False, ignore_engine=True)
    lines2 = normalize_schema(schema2, remove_replicated=False, ignore_engine=True)

    # Both should have the same engine placeholder
    engine_line1 = [line for line in lines1 if "ENGINE" in line][0]
    engine_line2 = [line for line in lines2 if "ENGINE" in line][0]

    assert engine_line1 == engine_line2
    assert "<ignored>" in engine_line1


def test_normalize_schema_ignore_engine_with_params() -> None:
    """Test ignoring engine with parameters."""
    schema1 = """CREATE TABLE test (
    id UInt64
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test', '{replica}')
ORDER BY id"""

    schema2 = """CREATE TABLE test (
    id UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY id"""

    lines1 = normalize_schema(schema1, remove_replicated=False, ignore_engine=True)
    lines2 = normalize_schema(schema2, remove_replicated=False, ignore_engine=True)

    # Both should have the same engine placeholder
    engine_line1 = [line for line in lines1 if "ENGINE" in line][0]
    engine_line2 = [line for line in lines2 if "ENGINE" in line][0]

    assert engine_line1 == engine_line2
    assert "<ignored>" in engine_line1


def test_normalize_schema_combined_ignore() -> None:
    """Test ignoring both UUID and engine."""
    schema1 = """CREATE TABLE test UUID '12345678-1234-1234-1234-123456789012' (
    id UInt64,
    name String
) ENGINE = MergeTree()
ORDER BY id"""

    schema2 = """CREATE TABLE test UUID 'abcdefab-abcd-abcd-abcd-abcdefabcdef' (
    id UInt64,
    name String
) ENGINE = ReplacingMergeTree()
ORDER BY id"""

    lines1 = normalize_schema(
        schema1,
        remove_replicated=False,
        ignore_uuid=True,
        ignore_engine=True,
    )
    lines2 = normalize_schema(
        schema2,
        remove_replicated=False,
        ignore_uuid=True,
        ignore_engine=True,
    )

    # Schemas should be identical after normalization
    assert lines1 == lines2


def test_normalize_schema_whitespace() -> None:
    """Test that extra whitespace is removed."""
    schema = """CREATE TABLE test (
    id UInt64,
    name String
) ENGINE = MergeTree()
ORDER BY id  """

    lines = normalize_schema(schema, remove_replicated=False)

    # Check that trailing whitespace is removed
    for line in lines:
        assert line == line.rstrip()


def test_normalize_schema_empty_lines() -> None:
    """Test that empty lines at start and end are removed."""
    schema = """

CREATE TABLE test (
    id UInt64
) ENGINE = MergeTree()
ORDER BY id

"""

    lines = normalize_schema(schema, remove_replicated=False)

    # First and last lines should not be empty
    assert lines[0] != ""
    assert lines[-1] != ""


def test_normalize_schema_all_options() -> None:
    """Test normalization with all options enabled."""
    schema = """

CREATE TABLE test UUID '12345678-1234-1234-1234-123456789012' (
    id UInt64,
    name String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test', '{replica}')
ORDER BY id

"""

    lines = normalize_schema(
        schema,
        remove_replicated=True,
        ignore_uuid=True,
        ignore_engine=True,
    )

    # Check that all normalizations are applied
    assert lines[0] != ""  # No empty lines at start
    assert lines[-1] != ""  # No empty lines at end

    uuid_line = [line for line in lines if "UUID" in line][0]
    assert "<ignored>" in uuid_line

    engine_line = [line for line in lines if "ENGINE" in line][0]
    assert "<ignored>" in engine_line

    # Check no trailing whitespace
    for line in lines:
        assert line == line.rstrip()
