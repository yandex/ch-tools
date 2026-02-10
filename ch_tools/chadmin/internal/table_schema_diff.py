"""
Module for comparing table schemas from different sources.
"""

import os
import re
from difflib import unified_diff
from typing import Dict, List, Tuple

from click import ClickException, Context
from termcolor import colored

from ch_tools.chadmin.internal.table import table_exists
from ch_tools.chadmin.internal.table_metadata import remove_replicated_params
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.chadmin.internal.zookeeper import check_zk_node, get_zk_node
from ch_tools.common import logging


def parse_source(source: str) -> Tuple[str, Dict[str, str]]:
    """
    Parse source string and determine its type.

    Args:
        source: Source identifier (e.g., "db.table", "/path/file.sql", "zk:/path")

    Returns:
        Tuple of (source_type, parameters)
        - ("clickhouse", {"database": "db", "table": "table"})
        - ("file", {"path": "/path/file.sql"})
        - ("zookeeper", {"path": "/clickhouse/tables/..."})
    """
    if source.startswith("zk:"):
        return ("zookeeper", {"path": source[3:]})
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
    result = execute_query(ctx, query, format_="TabSeparated")

    # Result is a plain string when format is TabSeparated
    if isinstance(result, str):
        return result.strip()

    # Fallback for other formats
    if isinstance(result, dict) and "data" in result:
        return result["data"][0]["statement"]

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


def get_schema_from_zookeeper(ctx: Context, zk_path: str) -> str:
    """
    Get CREATE TABLE statement from ZooKeeper.

    For ReplicatedMergeTree tables, the schema is stored in ZooKeeper
    at the replica path + "/metadata".

    Args:
        ctx: Click context
        zk_path: ZooKeeper path (can be replica path or direct metadata path)

    Returns:
        CREATE TABLE statement as string

    Raises:
        ClickException: If ZooKeeper node doesn't exist
    """
    # Try direct path first
    if not check_zk_node(ctx, zk_path):
        # Try appending /metadata
        metadata_path = os.path.join(zk_path, "metadata")
        if not check_zk_node(ctx, metadata_path):
            raise ClickException(f"ZooKeeper node not found: {zk_path}")
        zk_path = metadata_path

    try:
        return get_zk_node(ctx, zk_path)
    except Exception as e:
        raise ClickException(f"Failed to read from ZooKeeper {zk_path}: {e}")


def normalize_schema(
    schema: str,
    remove_replicated: bool = True,
    ignore_engine: bool = False,
    ignore_uuid: bool = False,
) -> List[str]:
    """
    Normalize CREATE TABLE statement for comparison.

    Normalization includes:
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
    # Remove ReplicatedMergeTree parameters if requested
    if remove_replicated:
        schema = remove_replicated_params(schema)

    # Remove UUID if requested
    if ignore_uuid:
        schema = re.sub(r"UUID\s+'[^']+'", "UUID '<ignored>'", schema)

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


def format_unified_diff(
    schema1_lines: List[str],
    schema2_lines: List[str],
    name1: str,
    name2: str,
    colored_output: bool = True,
    context_lines: int = 3,
) -> str:
    """
    Format schemas as unified diff.

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

    if not colored_output:
        return "\n".join(diff_lines)

    # Apply colors
    result = []
    for line in diff_lines:
        if line.startswith("---") or line.startswith("+++"):
            result.append(colored(line, "cyan", attrs=["bold"]))
        elif line.startswith("@@"):
            result.append(colored(line, "cyan"))
        elif line.startswith("-"):
            result.append(colored(line, "red"))
        elif line.startswith("+"):
            result.append(colored(line, "green"))
        else:
            result.append(line)

    return "\n".join(result)


def format_side_by_side_diff(
    schema1_lines: List[str],
    schema2_lines: List[str],
    name1: str,
    name2: str,
    colored_output: bool = True,
    width: int = 160,
) -> str:
    """
    Format schemas as side-by-side diff.

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

        # Truncate or pad lines
        line1_display = line1[:col_width].ljust(col_width)
        line2_display = line2[:col_width].ljust(col_width)

        # Apply colors if lines differ
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

            if line1 is not None and line2 is not None:
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
        source1: First source (db.table, /path/file.sql, or zk:/path)
        source2: Second source (db.table, /path/file.sql, or zk:/path)
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
    elif type1 == "file":
        schema1 = get_schema_from_file(params1["path"])
    else:  # zookeeper
        schema1 = get_schema_from_zookeeper(ctx, params1["path"])

    logging.info(f"Getting schema from {type2}: {source2}")
    if type2 == "clickhouse":
        schema2 = get_schema_from_clickhouse(ctx, params2["database"], params2["table"])
    elif type2 == "file":
        schema2 = get_schema_from_file(params2["path"])
    else:  # zookeeper
        schema2 = get_schema_from_zookeeper(ctx, params2["path"])

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
