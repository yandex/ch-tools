import re
from typing import Mapping, Union

from .sql import TableRef


class Identifier(str):
    """A database, table or column name passed to a native Identifier parameter."""


Parameter = Union[str, int, list[str]]
Parameters = Mapping[str, Parameter]


def table_parameters(database: str, table: str) -> dict[str, Parameter]:
    return {"db": Identifier(database), "tbl": Identifier(table)}


def table_ref_parameters(table: TableRef, prefix: str = "") -> dict[str, Parameter]:
    return {
        prefix + key: value
        for key, value in table_parameters(table.database, table.table).items()
    }


_ESCAPED_TEXT = {
    ord("\\"): r"\\",
    ord("\t"): r"\t",
    ord("\n"): r"\n",
    ord("\r"): r"\r",
    ord("\0"): r"\0",
}


def encode_parameters(values: Parameters) -> dict[str, str]:
    """Encode native parameters for ClickHouse's text protocol."""
    result = {}
    for name, value in values.items():
        if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", name):
            raise ValueError(f"Invalid ClickHouse parameter name: {name!r}")
        if isinstance(value, Identifier):
            encoded = str(value)
        elif isinstance(value, str):
            encoded = value.translate(_ESCAPED_TEXT)
        elif isinstance(value, list):
            items = (
                item.translate(_ESCAPED_TEXT).replace("'", r"\'") for item in value
            )
            encoded = "[" + ",".join("'" + item + "'" for item in items) + "]"
        elif isinstance(value, int):
            encoded = str(int(value))
        else:
            raise TypeError(
                f"Unsupported ClickHouse parameter type: {type(value).__name__}"
            )
        result[name] = encoded
    return result
