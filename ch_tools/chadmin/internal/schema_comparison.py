"""
Module for comparing ClickHouse table schemas.

Provides unified functions for normalizing and comparing table schemas
that can be used across different project modules.
"""

import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

from ch_tools.chadmin.internal.schema_formatters import UnifiedDiffFormatter
from ch_tools.chadmin.internal.table_metadata import remove_replicated_params


@dataclass
class SchemaComparisonResult:
    """
    Result of comparing two table schemas.

    Attributes:
        are_equal: True if schemas are identical after normalization
        normalized_schema1: Normalized lines of the first schema
        normalized_schema2: Normalized lines of the second schema
        differences: List of differences in format (line_number, line1, line2)
    """

    are_equal: bool
    normalized_schema1: List[str]
    normalized_schema2: List[str]
    differences: Optional[List[Tuple[int, str, str]]] = None


def normalize_schema(
    schema: str,
    remove_replicated: bool = True,
    ignore_engine: bool = False,
    ignore_uuid: bool = False,
) -> List[str]:
    """
    Normalize CREATE TABLE statement for comparison.

    Normalization includes:
    - Converting ATTACH TABLE to CREATE TABLE
    - Normalizing table names to a common placeholder
    - Removing extra whitespace
    - Optionally: removing ReplicatedMergeTree parameters
    - Optionally: ignoring engine differences
    - Optionally: ignoring UUID differences

    Args:
        schema: CREATE TABLE statement
        remove_replicated: Remove ReplicatedMergeTree parameters
        ignore_engine: Ignore engine differences
        ignore_uuid: Ignore UUID differences

    Returns:
        List of normalized lines
    """
    # Normalize ATTACH TABLE to CREATE TABLE
    schema = re.sub(r"^ATTACH TABLE", "CREATE TABLE", schema, flags=re.MULTILINE)

    # Normalize table name to placeholder
    schema = re.sub(
        r"(CREATE TABLE\s+)(?:`?[\w.]+`?|_)", r"\1<table>", schema, flags=re.MULTILINE
    )

    # Remove ReplicatedMergeTree parameters if requested
    if remove_replicated:
        schema = remove_replicated_params(schema)

    # Remove UUID if requested
    if ignore_uuid:
        # Remove UUID from any position in schema (not just from ATTACH)
        schema = re.sub(r"UUID\s+'[^']+'", "", schema)

    # Remove engine if requested
    if ignore_engine:
        # Replace ENGINE = ... with ENGINE = <ignored>
        schema = re.sub(
            r"ENGINE\s*=\s*\w+(?:\([^)]*\))?",
            "ENGINE = <ignored>",
            schema,
        )

    # Split into lines and remove trailing whitespace
    lines = [line.rstrip() for line in schema.split("\n")]

    # Remove empty lines at the beginning and end
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()

    return lines


def compare_schemas_simple(
    schema1: str,
    schema2: str,
    ignore_uuid: bool = True,
    ignore_engine: bool = False,
    remove_replicated: bool = True,
) -> bool:
    """
    Simple comparison of two table schemas.

    This function is intended for use cases where only a boolean
    comparison result (equal/not equal) is needed, without detailed
    information about differences.

    Args:
        schema1: First schema (CREATE TABLE statement)
        schema2: Second schema (CREATE TABLE statement)
        ignore_uuid: Ignore UUID differences
        ignore_engine: Ignore engine differences
        remove_replicated: Remove ReplicatedMergeTree parameters

    Returns:
        True if schemas are identical after normalization, False otherwise

    Example:
        >>> schema1 = "CREATE TABLE test UUID '123' (id UInt64) ENGINE = MergeTree()"
        >>> schema2 = "CREATE TABLE test UUID '456' (id UInt64) ENGINE = MergeTree()"
        >>> compare_schemas_simple(schema1, schema2, ignore_uuid=True)
        True
    """
    normalized1 = normalize_schema(
        schema1,
        remove_replicated=remove_replicated,
        ignore_engine=ignore_engine,
        ignore_uuid=ignore_uuid,
    )
    normalized2 = normalize_schema(
        schema2,
        remove_replicated=remove_replicated,
        ignore_engine=ignore_engine,
        ignore_uuid=ignore_uuid,
    )

    return normalized1 == normalized2


def get_schema_differences(
    schema1_lines: List[str],
    schema2_lines: List[str],
) -> List[Tuple[int, str, str]]:
    """
    Get list of differences between two normalized schemas.

    Args:
        schema1_lines: Lines of the first schema
        schema2_lines: Lines of the second schema

    Returns:
        List of tuples (line_number, line1, line2) for differing lines.
        If one schema is shorter, missing lines are represented as empty strings.

    Example:
        >>> lines1 = ["CREATE TABLE test (", "  id UInt64", ")"]
        >>> lines2 = ["CREATE TABLE test (", "  id UInt32", ")"]
        >>> diffs = get_schema_differences(lines1, lines2)
        >>> diffs
        [(1, '  id UInt64', '  id UInt32')]
    """
    differences = []
    max_lines = max(len(schema1_lines), len(schema2_lines))

    for i in range(max_lines):
        line1 = schema1_lines[i] if i < len(schema1_lines) else ""
        line2 = schema2_lines[i] if i < len(schema2_lines) else ""

        if line1 != line2:
            differences.append((i, line1, line2))

    return differences


def compare_schemas_detailed(
    schema1: str,
    schema2: str,
    ignore_uuid: bool = True,
    ignore_engine: bool = False,
    remove_replicated: bool = True,
) -> SchemaComparisonResult:
    """
    Detailed comparison of two table schemas.

    This function returns complete information about the comparison, including
    normalized schemas and a list of differences.

    Args:
        schema1: First schema (CREATE TABLE statement)
        schema2: Second schema (CREATE TABLE statement)
        ignore_uuid: Ignore UUID differences
        ignore_engine: Ignore engine differences
        remove_replicated: Remove ReplicatedMergeTree parameters

    Returns:
        SchemaComparisonResult with detailed comparison information

    Example:
        >>> schema1 = "CREATE TABLE test (id UInt64) ENGINE = MergeTree()"
        >>> schema2 = "CREATE TABLE test (id UInt32) ENGINE = MergeTree()"
        >>> result = compare_schemas_detailed(schema1, schema2)
        >>> result.are_equal
        False
        >>> len(result.differences)
        1
    """
    normalized1 = normalize_schema(
        schema1,
        remove_replicated=remove_replicated,
        ignore_engine=ignore_engine,
        ignore_uuid=ignore_uuid,
    )
    normalized2 = normalize_schema(
        schema2,
        remove_replicated=remove_replicated,
        ignore_engine=ignore_engine,
        ignore_uuid=ignore_uuid,
    )

    are_equal = normalized1 == normalized2
    differences = (
        None if are_equal else get_schema_differences(normalized1, normalized2)
    )

    return SchemaComparisonResult(
        are_equal=are_equal,
        normalized_schema1=normalized1,
        normalized_schema2=normalized2,
        differences=differences,
    )


def generate_schema_diff(
    schema1: str,
    schema2: str,
    name1: str,
    name2: str,
    colored_output: bool = True,
    ignore_uuid: bool = True,
    ignore_engine: bool = False,
    remove_replicated: bool = True,
    context_lines: int = 3,
) -> str:
    """
    Generate a formatted diff between two schemas.

    This is a convenience function that combines normalization and formatting
    in one call. Useful for logging and error reporting.

    Args:
        schema1: First schema (CREATE TABLE statement)
        schema2: Second schema (CREATE TABLE statement)
        name1: Label for first schema (e.g., "Local", "Host1")
        name2: Label for second schema (e.g., "ZooKeeper", "Host2")
        colored_output: Whether to use colored output
        ignore_uuid: Ignore UUID differences
        ignore_engine: Ignore engine differences
        remove_replicated: Remove ReplicatedMergeTree parameters
        context_lines: Number of context lines for unified diff

    Returns:
        Formatted unified diff as string

    Example:
        >>> diff = generate_schema_diff(
        ...     local_schema,
        ...     zk_schema,
        ...     "Local",
        ...     "ZooKeeper",
        ...     colored_output=True
        ... )
        >>> logging.error(f"Schema mismatch:\\n{diff}")
    """

    # Normalize schemas
    schema1_lines = normalize_schema(
        schema1,
        remove_replicated=remove_replicated,
        ignore_engine=ignore_engine,
        ignore_uuid=ignore_uuid,
    )
    schema2_lines = normalize_schema(
        schema2,
        remove_replicated=remove_replicated,
        ignore_engine=ignore_engine,
        ignore_uuid=ignore_uuid,
    )

    # Format diff
    formatter = UnifiedDiffFormatter(colored_output=colored_output)
    return formatter.format(
        schema1_lines,
        schema2_lines,
        name1,
        name2,
        context_lines=context_lines,
    )
