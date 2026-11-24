from dataclasses import dataclass


def quote_identifier(value: str) -> str:
    """Quote a ClickHouse identifier."""
    escaped = value.replace("\\", "\\\\").replace("`", "``")
    return f"`{escaped}`"


def quote_string(value: str) -> str:
    """Quote a ClickHouse string literal."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def qualified_name(database: str, table: str) -> str:
    """Quote each component of a ClickHouse object name."""
    return f"{quote_identifier(database)}.{quote_identifier(table)}"


@dataclass(frozen=True)
class TableRef:
    """Qualified ClickHouse table reference."""

    database: str
    table: str

    @property
    def sql(self) -> str:
        return qualified_name(self.database, self.table)

    @property
    def display(self) -> str:
        return f"{self.database}.{self.table}"
