from typing import Optional

from .sql import quote_string


def _format_str_match(value: Optional[str], exact: bool = False) -> Optional[str]:
    if value is None:
        return None

    if exact:
        return f"= {quote_string(value)}"

    if value.find(",") < 0:
        return f"LIKE {quote_string(value)}"

    return "IN ({0})".format(
        ",".join(quote_string(item.strip()) for item in value.split(","))
    )


def _format_str_imatch(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None

    return _format_str_match(value.lower())
