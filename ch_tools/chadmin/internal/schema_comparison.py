"""
Модуль для сравнения схем таблиц ClickHouse.

Предоставляет унифицированные функции для нормализации и сравнения схем таблиц,
которые могут быть использованы в различных модулях проекта.
"""

import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

from ch_tools.chadmin.internal.schema_formatters import UnifiedDiffFormatter
from ch_tools.chadmin.internal.table_metadata import remove_replicated_params


@dataclass
class SchemaComparisonResult:
    """
    Результат сравнения двух схем таблиц.

    Attributes:
        are_equal: True если схемы идентичны после нормализации
        normalized_schema1: Нормализованные строки первой схемы
        normalized_schema2: Нормализованные строки второй схемы
        differences: Список различий в формате (номер_строки, строка1, строка2)
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
    Нормализовать CREATE TABLE statement для сравнения.

    Нормализация включает:
    - Преобразование ATTACH TABLE в CREATE TABLE
    - Нормализацию имен таблиц в общий placeholder
    - Удаление лишних пробелов
    - Опционально: удаление параметров ReplicatedMergeTree
    - Опционально: игнорирование различий в engine
    - Опционально: игнорирование различий в UUID

    Args:
        schema: CREATE TABLE statement
        remove_replicated: Удалить параметры ReplicatedMergeTree
        ignore_engine: Игнорировать различия в engine
        ignore_uuid: Игнорировать различия в UUID

    Returns:
        Список нормализованных строк
    """
    # Нормализация ATTACH TABLE в CREATE TABLE
    schema = re.sub(r"^ATTACH TABLE", "CREATE TABLE", schema, flags=re.MULTILINE)

    # Нормализация имени таблицы в placeholder
    schema = re.sub(
        r"(CREATE TABLE\s+)(?:`?[\w.]+`?|_)", r"\1<table>", schema, flags=re.MULTILINE
    )

    # Удаление параметров ReplicatedMergeTree если запрошено
    if remove_replicated:
        schema = remove_replicated_params(schema)

    # Удаление UUID если запрошено
    if ignore_uuid:
        # Удаляем UUID из любой позиции в схеме (не только из ATTACH)
        schema = re.sub(r"UUID\s+'[^']+'", "", schema)

    # Удаление engine если запрошено
    if ignore_engine:
        # Заменить ENGINE = ... на ENGINE = <ignored>
        schema = re.sub(
            r"ENGINE\s*=\s*\w+(?:\([^)]*\))?",
            "ENGINE = <ignored>",
            schema,
        )

    # Разбить на строки и удалить trailing whitespace
    lines = [line.rstrip() for line in schema.split("\n")]

    # Удалить пустые строки в начале и конце
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
    Простое сравнение двух схем таблиц.

    Эта функция предназначена для использования в случаях, когда нужен
    только булев результат сравнения (равны/не равны), без детальной
    информации о различиях.

    Args:
        schema1: Первая схема (CREATE TABLE statement)
        schema2: Вторая схема (CREATE TABLE statement)
        ignore_uuid: Игнорировать различия в UUID
        ignore_engine: Игнорировать различия в engine
        remove_replicated: Удалить параметры ReplicatedMergeTree

    Returns:
        True если схемы идентичны после нормализации, False иначе

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
    Получить список различий между двумя нормализованными схемами.

    Args:
        schema1_lines: Строки первой схемы
        schema2_lines: Строки второй схемы

    Returns:
        Список кортежей (номер_строки, строка1, строка2) для различающихся строк.
        Если одна схема короче, недостающие строки представлены как пустые строки.

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
    Детальное сравнение двух схем таблиц.

    Эта функция возвращает полную информацию о сравнении, включая
    нормализованные схемы и список различий.

    Args:
        schema1: Первая схема (CREATE TABLE statement)
        schema2: Вторая схема (CREATE TABLE statement)
        ignore_uuid: Игнорировать различия в UUID
        ignore_engine: Игнорировать различия в engine
        remove_replicated: Удалить параметры ReplicatedMergeTree

    Returns:
        SchemaComparisonResult с детальной информацией о сравнении

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
