"""
Module for comparing table schemas from different sources.
"""

import os
import re
from difflib import SequenceMatcher, unified_diff
from typing import Dict, List, Tuple

from click import ClickException, Context
from termcolor import colored

from ch_tools.chadmin.internal.table import table_exists
from ch_tools.chadmin.internal.table_metadata import remove_replicated_params
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging


def parse_source(source: str) -> Tuple[str, Dict[str, str]]:
    """
    Parse source string and determine its type.

    Args:
        source: Source identifier (e.g., "db.table", "/path/file.sql")

    Returns:
        Tuple of (source_type, parameters)
        - ("clickhouse", {"database": "db", "table": "table"})
        - ("file", {"path": "/path/file.sql"})
    """
    if "." in source and not source.startswith("/") and not source.startswith("~"):
        parts = source.split(".", 1)
        return ("clickhouse", {"database": parts[0], "table": parts[1]})
    return ("file", {"path": os.path.expanduser(source)})


def get_schema_from_clickhouse(ctx: Context, database: str, table: str) -> str:
    """
    Get CREATE TABLE statement from ClickHouse.

    Args:
        ctx: Click context
        database: Database name
        table: Table name

    Returns:
        CREATE TABLE statement as string

    Raises:
        ClickException: If table doesn't exist
    """
    if not table_exists(ctx, database, table):
        raise ClickException(f"Table `{database}`.`{table}` not found in ClickHouse")

    query = f"SHOW CREATE TABLE `{database}`.`{table}`"
    result = execute_query(ctx, query, format_="TSVRaw")

    if isinstance(result, str):
        return result

    return str(result).strip()


def get_schema_from_file(file_path: str) -> str:
    """
    Read CREATE TABLE statement from file.

    Args:
        file_path: Path to file containing CREATE TABLE statement

    Returns:
        CREATE TABLE statement as string

    Raises:
        ClickException: If file doesn't exist or can't be read
    """
    if not os.path.exists(file_path):
        raise ClickException(f"File not found: {file_path}")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        raise ClickException(f"Failed to read file {file_path}: {e}")


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
    - Consistent formatting
    - Optionally removing ReplicatedMergeTree parameters
    - Optionally ignoring engine differences
    - Optionally ignoring UUID differences

    Args:
        schema: CREATE TABLE statement
        remove_replicated: Whether to remove ReplicatedMergeTree parameters
        ignore_engine: Whether to ignore engine differences
        ignore_uuid: Whether to ignore UUID differences

    Returns:
        List of normalized lines
    """
    # Normalize ATTACH TABLE to CREATE TABLE
    schema = re.sub(r"^ATTACH TABLE", "CREATE TABLE", schema, flags=re.MULTILINE)

    # Normalize table name to a placeholder (handles both CREATE TABLE name and CREATE TABLE db.name)
    # This regex captures the table name after CREATE TABLE and replaces it with a placeholder
    schema = re.sub(
        r"(CREATE TABLE\s+)(?:`?[\w.]+`?|_)", r"\1<table>", schema, flags=re.MULTILINE
    )

    # Remove ReplicatedMergeTree parameters if requested
    if remove_replicated:
        schema = remove_replicated_params(schema)

    # Remove UUID if requested
    if ignore_uuid:
        schema = re.sub(r"UUID\s+'[^']+'", "", schema)

    # Remove engine if requested
    if ignore_engine:
        # Replace ENGINE = ... with ENGINE = <ignored>
        # Match ENGINE = followed by engine name and optional parameters
        schema = re.sub(
            r"ENGINE\s*=\s*\w+(?:\([^)]*\))?",
            "ENGINE = <ignored>",
            schema,
        )

    # Split into lines and strip whitespace
    lines = [line.rstrip() for line in schema.split("\n")]

    # Remove empty lines at start and end
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()

    return lines


def highlight_line_differences(
    line1: str, line2: str, colored_output: bool = True
) -> Tuple[str, str, str]:
    """
    Highlight character-level differences between two lines.

    Args:
        line1: First line
        line2: Second line
        colored_output: Whether to use colored output

    Returns:
        Tuple of (highlighted_line1, highlighted_line2, marker_line)
        marker_line contains "^" symbols under differing characters
    """
    if line1 == line2:
        return line1, line2, ""

    matcher = SequenceMatcher(None, line1, line2)

    result1 = []
    result2 = []
    markers = []

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            # Equal parts - keep base color but no bold
            text1 = line1[i1:i2]
            text2 = line2[j1:j2]
            if colored_output:
                result1.append(colored(text1, "red"))
                result2.append(colored(text2, "green"))
            else:
                # In non-colored mode, just append plain text
                result1.append(text1)
                result2.append(text2)
            markers.append(" " * (i2 - i1))
        elif tag == "replace":
            # Highlight replaced text with bold on colored background
            text1 = line1[i1:i2]
            text2 = line2[j1:j2]
            if colored_output:
                # Use bold for emphasis on top of base color
                result1.append(colored(text1, "red", attrs=["bold"]))
                result2.append(colored(text2, "green", attrs=["bold"]))
            else:
                # In non-colored mode, just append plain text (markers will show differences)
                result1.append(text1)
                result2.append(text2)
            # Add markers for the longer segment
            max_len = max(i2 - i1, j2 - j1)
            markers.append("^" * max_len)
        elif tag == "delete":
            # Text only in line1 - bold on red background
            text1 = line1[i1:i2]
            if colored_output:
                result1.append(colored(text1, "red", attrs=["bold"]))
            else:
                # In non-colored mode, just append plain text
                result1.append(text1)
            markers.append("^" * (i2 - i1))
        elif tag == "insert":
            # Text only in line2 - bold on green background
            text2 = line2[j1:j2]
            if colored_output:
                result2.append(colored(text2, "green", attrs=["bold"]))
            else:
                # In non-colored mode, just append plain text
                result2.append(text2)
            # For inserts, we need to pad result1 and add markers
            pad_len = j2 - j1
            result1.append(" " * pad_len)
            markers.append("^" * pad_len)

    marker_line = "".join(markers).rstrip()

    return "".join(result1), "".join(result2), marker_line


def format_unified_diff(  # pylint: disable=too-many-branches
    schema1_lines: List[str],
    schema2_lines: List[str],
    name1: str,
    name2: str,
    colored_output: bool = True,
    context_lines: int = 3,
) -> str:
    """
    Format schemas as unified diff with character-level highlighting.

    Args:
        schema1_lines: First schema as list of lines
        schema2_lines: Second schema as list of lines
        name1: Name/label for first schema
        name2: Name/label for second schema
        colored_output: Whether to use colored output
        context_lines: Number of context lines

    Returns:
        Formatted unified diff as string
    """
    diff_lines = list(
        unified_diff(
            schema1_lines,
            schema2_lines,
            fromfile=name1,
            tofile=name2,
            lineterm="",
            n=context_lines,
        )
    )

    if not diff_lines:
        return "Schemas are identical"

    # Apply colors and character-level highlighting
    result = []
    i = 0
    while i < len(diff_lines):
        line = diff_lines[i]

        if line.startswith("---") or line.startswith("+++"):
            if colored_output:
                result.append(colored(line, "cyan", attrs=["bold"]))
            else:
                result.append(line)
        elif line.startswith("@@"):
            if colored_output:
                result.append(colored(line, "cyan"))
            else:
                result.append(line)
        elif (
            line.startswith("-")
            and i + 1 < len(diff_lines)
            and diff_lines[i + 1].startswith("+")
        ):
            # We have a pair of changed lines - highlight character differences
            line1 = line[1:]  # Remove '-' prefix
            line2 = diff_lines[i + 1][1:]  # Remove '+' prefix

            highlighted1, highlighted2, markers = highlight_line_differences(
                line1, line2, colored_output
            )

            result.append("-" + highlighted1)
            result.append("+" + highlighted2)
            # Show markers only in non-colored mode
            if not colored_output and markers:
                result.append(" " + markers)

            i += 2  # Skip next line as we've processed it
            continue
        elif line.startswith("-"):
            if colored_output:
                result.append(colored(line, "red"))
            else:
                result.append(line)
        elif line.startswith("+"):
            if colored_output:
                result.append(colored(line, "green"))
            else:
                result.append(line)
        else:
            result.append(line)

        i += 1

    return "\n".join(result)


def _visible_length(text: str) -> int:
    """
    Calculate the visible length of a string, excluding ANSI escape sequences.

    Args:
        text: String that may contain ANSI escape sequences

    Returns:
        Visible length of the string
    """
    # Remove ANSI escape sequences
    ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
    return len(ansi_escape.sub("", text))


def _pad_to_width(text: str, width: int) -> str:
    """
    Pad a string to the specified width, accounting for ANSI escape sequences.

    Args:
        text: String that may contain ANSI escape sequences
        width: Target width

    Returns:
        Padded string
    """
    visible_len = _visible_length(text)
    if visible_len >= width:
        # Truncate if too long - need to be careful with escape sequences
        # For now, just return as is if it's too long
        return text
    # Add padding
    padding = " " * (width - visible_len)
    return text + padding


def format_side_by_side_diff(
    schema1_lines: List[str],
    schema2_lines: List[str],
    name1: str,
    name2: str,
    colored_output: bool = True,
    width: int = 160,
) -> str:
    """
    Format schemas as side-by-side diff with character-level highlighting.

    Args:
        schema1_lines: First schema as list of lines
        schema2_lines: Second schema as list of lines
        name1: Name/label for first schema
        name2: Name/label for second schema
        colored_output: Whether to use colored output
        width: Total width for output

    Returns:
        Formatted side-by-side diff as string
    """
    col_width = (width - 3) // 2  # 3 chars for separator " | "

    result = []

    # Header
    header1 = name1[:col_width].ljust(col_width)
    header2 = name2[:col_width].ljust(col_width)
    if colored_output:
        header1 = colored(header1, "cyan", attrs=["bold"])
        header2 = colored(header2, "cyan", attrs=["bold"])
    result.append(f"{header1} | {header2}")
    result.append("-" * col_width + " | " + "-" * col_width)

    # Compare line by line
    max_lines = max(len(schema1_lines), len(schema2_lines))

    for i in range(max_lines):
        line1 = schema1_lines[i] if i < len(schema1_lines) else ""
        line2 = schema2_lines[i] if i < len(schema2_lines) else ""

        # Apply character-level highlighting if lines differ
        if line1 != line2 and line1 and line2:
            highlighted1, highlighted2, markers = highlight_line_differences(
                line1, line2, colored_output
            )
            # Truncate to column width based on visible length
            if colored_output:
                if _visible_length(highlighted1) > col_width:
                    highlighted1 = line1[:col_width]
                    highlighted1, _, _ = highlight_line_differences(
                        highlighted1,
                        line2[:col_width] if len(line2) > col_width else line2,
                        colored_output,
                    )
                if _visible_length(highlighted2) > col_width:
                    highlighted2 = line2[:col_width]
                    _, highlighted2, _ = highlight_line_differences(
                        line1[:col_width] if len(line1) > col_width else line1,
                        highlighted2,
                        colored_output,
                    )
                line1_display = _pad_to_width(highlighted1, col_width)
                line2_display = _pad_to_width(highlighted2, col_width)
            else:
                # In non-colored mode, show markers under differences
                line1_display = highlighted1[:col_width].ljust(col_width)
                line2_display = highlighted2[:col_width].ljust(col_width)

            result.append(f"{line1_display} | {line2_display}")

            # Add marker line in non-colored mode (only under the line with differences)
            if not colored_output and markers:
                markers_truncated = markers[:col_width]
                # Show markers in both columns for clarity
                markers_display = markers_truncated.ljust(col_width)
                result.append(f"{markers_display} | {markers_display}")
        else:
            # Truncate or pad lines
            line1_truncated = line1[:col_width]
            line2_truncated = line2[:col_width]

            line1_display = line1_truncated.ljust(col_width)
            line2_display = line2_truncated.ljust(col_width)

            # Apply colors if lines differ (but no character highlighting)
            if colored_output and line1 != line2:
                if line1 and not line2:
                    line1_display = colored(line1_display, "red")
                elif line2 and not line1:
                    line2_display = colored(line2_display, "green")
                elif line1 != line2:
                    line1_display = colored(line1_display, "red")
                    line2_display = colored(line2_display, "green")

            result.append(f"{line1_display} | {line2_display}")

    return "\n".join(result)


def format_normal_diff(
    schema1_lines: List[str],
    schema2_lines: List[str],
    colored_output: bool = True,
) -> str:
    """
    Format schemas as normal diff.

    Args:
        schema1_lines: First schema as list of lines
        schema2_lines: Second schema as list of lines
        colored_output: Whether to use colored output

    Returns:
        Formatted normal diff as string
    """
    if schema1_lines == schema2_lines:
        return "Schemas are identical"

    result = []

    # Simple line-by-line comparison
    max_lines = max(len(schema1_lines), len(schema2_lines))

    for i in range(max_lines):
        line1 = schema1_lines[i] if i < len(schema1_lines) else None
        line2 = schema2_lines[i] if i < len(schema2_lines) else None

        if line1 != line2:
            if line1 is not None and line2 is not None:
                # Both lines exist - show character-level differences
                highlighted1, highlighted2, markers = highlight_line_differences(
                    line1, line2, colored_output
                )

                prefix1 = f"< {highlighted1}"
                prefix2 = f"> {highlighted2}"

                if colored_output:
                    # Already colored by highlight_line_differences
                    result.append(prefix1)
                    result.append(prefix2)
                else:
                    # In non-colored mode, show markers
                    result.append(prefix1)
                    result.append(prefix2)
                    if markers:
                        result.append(
                            "  " + markers
                        )  # 2 spaces to align with "< " and "> "

                result.append("---")
            else:
                # Only one line exists
                if line1 is not None:
                    prefix = f"< {line1}"
                    if colored_output:
                        prefix = colored(prefix, "red")
                    result.append(prefix)

                if line2 is not None:
                    prefix = f"> {line2}"
                    if colored_output:
                        prefix = colored(prefix, "green")
                    result.append(prefix)

                if line1 is not None or line2 is not None:
                    result.append("---")

    return "\n".join(result) if result else "Schemas are identical"


def compare_schemas(
    ctx: Context,
    source1: str,
    source2: str,
    diff_format: str = "unified",
    colored_output: bool = True,
    normalize: bool = True,
    context_lines: int = 3,
    ignore_engine: bool = False,
    ignore_uuid: bool = False,
) -> str:
    """
    Compare schemas from two sources.

    Args:
        ctx: Click context
        source1: First source (db.table or /path/file.sql)
        source2: Second source (db.table or /path/file.sql)
        diff_format: Output format ("unified", "side-by-side", or "normal")
        colored_output: Whether to use colored output
        normalize: Whether to normalize schemas before comparison
        context_lines: Number of context lines for unified diff
        ignore_engine: Whether to ignore engine differences
        ignore_uuid: Whether to ignore UUID differences

    Returns:
        Formatted diff as string
    """
    # Parse sources
    type1, params1 = parse_source(source1)
    type2, params2 = parse_source(source2)

    # Get schemas
    logging.info(f"Getting schema from {type1}: {source1}")
    if type1 == "clickhouse":
        schema1 = get_schema_from_clickhouse(ctx, params1["database"], params1["table"])
    else:  # file
        schema1 = get_schema_from_file(params1["path"])

    logging.info(f"Getting schema from {type2}: {source2}")
    if type2 == "clickhouse":
        schema2 = get_schema_from_clickhouse(ctx, params2["database"], params2["table"])
    else:  # file
        schema2 = get_schema_from_file(params2["path"])

    # Normalize if requested
    if normalize:
        schema1_lines = normalize_schema(
            schema1,
            remove_replicated=True,
            ignore_engine=ignore_engine,
            ignore_uuid=ignore_uuid,
        )
        schema2_lines = normalize_schema(
            schema2,
            remove_replicated=True,
            ignore_engine=ignore_engine,
            ignore_uuid=ignore_uuid,
        )
    else:
        schema1_lines = schema1.split("\n")
        schema2_lines = schema2.split("\n")

    # Format diff
    if diff_format == "unified":
        return format_unified_diff(
            schema1_lines,
            schema2_lines,
            source1,
            source2,
            colored_output,
            context_lines,
        )
    if diff_format == "side-by-side":
        return format_side_by_side_diff(
            schema1_lines, schema2_lines, source1, source2, colored_output
        )
    # normal
    return format_normal_diff(schema1_lines, schema2_lines, colored_output)
