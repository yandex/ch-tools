"""
Module for comparing table schemas from different sources.
"""

import os
from typing import Dict, Tuple, Type

from click import ClickException, Context

from ch_tools.chadmin.internal.schema_comparison import normalize_schema
from ch_tools.chadmin.internal.schema_formatters import (
    BaseSchemaFormatter,
    NormalDiffFormatter,
    SideBySideDiffFormatter,
    UnifiedDiffFormatter,
)
from ch_tools.chadmin.internal.table import table_exists
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
    # Check if it's a file path (absolute, relative, or with tilde)
    # Relative paths like ./file.sql or ../file.sql should be treated as files
    if (
        source.startswith("/")
        or source.startswith("~")
        or source.startswith("./")
        or source.startswith("../")
        or os.path.exists(source)
    ):
        return ("file", {"path": os.path.expanduser(source)})

    # If it contains a dot and is not a file path, treat as database.table
    if "." in source:
        parts = source.split(".", 1)
        return ("clickhouse", {"database": parts[0], "table": parts[1]})

    # Default to file path
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
        FileNotFoundError: If file doesn't exist
        PermissionError: If file can't be read due to permissions
        UnicodeDecodeError: If file can't be decoded as UTF-8
        IOError: If file can't be read for other reasons
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()
    except PermissionError as e:
        raise PermissionError(f"Permission denied reading file {file_path}: {e}") from e
    except UnicodeDecodeError as e:
        raise UnicodeDecodeError(
            e.encoding,
            e.object,
            e.start,
            e.end,
            f"Failed to decode file {file_path}: {e.reason}",
        ) from e
    except IOError as e:
        raise IOError(f"Failed to read file {file_path}: {e}") from e


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

    # Format diff using appropriate formatter
    formatters: Dict[str, Type[BaseSchemaFormatter]] = {
        "unified": UnifiedDiffFormatter,
        "side-by-side": SideBySideDiffFormatter,
        "normal": NormalDiffFormatter,
    }

    formatter_class = formatters.get(diff_format, UnifiedDiffFormatter)
    formatter = formatter_class(colored_output=colored_output)

    return formatter.format(
        schema1_lines,
        schema2_lines,
        source1,
        source2,
        context_lines=context_lines,
    )
