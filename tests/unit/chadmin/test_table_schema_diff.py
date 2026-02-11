"""
Unit tests for table schema diff functionality.
"""

from ch_tools.chadmin.internal.table_schema_diff import (
    highlight_line_differences,
    normalize_schema,
)


def test_normalize_schema_basic() -> None:
    """Test basic schema normalization."""
    schema = """CREATE TABLE test (
    id UInt64,
    name String
) ENGINE = MergeTree()
ORDER BY id"""

    lines = normalize_schema(schema, remove_replicated=False)

    assert len(lines) > 0
    # Table name is normalized to <table>
    assert lines[0] == "CREATE TABLE <table> ("


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

    # Both should be identical after UUID removal
    assert lines1 == lines2
    # UUID should not be present in normalized output
    assert not any("UUID" in line for line in lines1)


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
        ignore_engine=True,
        ignore_uuid=True,
    )
    lines2 = normalize_schema(
        schema2,
        remove_replicated=False,
        ignore_engine=True,
        ignore_uuid=True,
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
        ignore_engine=True,
        ignore_uuid=True,
    )

    # Check that all normalizations are applied
    assert lines[0] != ""  # No empty lines at start
    assert lines[-1] != ""  # No empty lines at end

    # UUID should be removed
    assert not any("UUID" in line for line in lines)

    engine_line = [line for line in lines if "ENGINE" in line][0]
    assert "<ignored>" in engine_line

    # Check no trailing whitespace
    for line in lines:
        assert line == line.rstrip()


def test_highlight_line_differences_identical() -> None:
    """Test highlighting when lines are identical."""
    line1 = "CREATE TABLE test ("
    line2 = "CREATE TABLE test ("

    result1, result2, markers = highlight_line_differences(
        line1, line2, colored_output=False
    )

    assert result1 == line1
    assert result2 == line2
    assert markers == ""


def test_highlight_line_differences_replace() -> None:
    """Test highlighting replaced text."""
    line1 = "ENGINE = MergeTree()"
    line2 = "ENGINE = ReplacingMergeTree()"

    result1, result2, markers = highlight_line_differences(
        line1, line2, colored_output=False
    )

    # Markers should indicate differences
    assert "^" in markers
    # Results should contain the original text (without colors in this test)
    assert "MergeTree" in result1
    assert "ReplacingMergeTree" in result2


def test_highlight_line_differences_insert() -> None:
    """Test highlighting inserted text."""
    line1 = "id UInt64"
    line2 = "id UInt64, name String"

    result1, result2, markers = highlight_line_differences(
        line1, line2, colored_output=False
    )

    # Markers should indicate the insertion
    assert "^" in markers
    assert "name String" in result2


def test_highlight_line_differences_delete() -> None:
    """Test highlighting deleted text."""
    line1 = "id UInt64, name String"
    line2 = "id UInt64"

    result1, result2, markers = highlight_line_differences(
        line1, line2, colored_output=False
    )

    # Markers should indicate the deletion
    assert "^" in markers
    assert "name String" in result1


def test_highlight_line_differences_multiple_changes() -> None:
    """Test highlighting multiple changes in one line."""
    line1 = "CREATE TABLE old_name (id Int32)"
    line2 = "CREATE TABLE new_name (id Int64)"

    result1, result2, markers = highlight_line_differences(
        line1, line2, colored_output=False
    )

    # Should have markers for both changes
    assert "^" in markers
    # Both differences should be present
    assert "old_name" in result1
    assert "new_name" in result2
    assert "Int32" in result1
    assert "Int64" in result2
