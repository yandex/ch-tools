from typing import Any, Callable, Mapping, Optional

from jinja2 import Environment

from ch_tools.common.utils import version_ge

from .parameters import Identifier, Parameter
from .sql import quote_identifier, quote_string


def _format_str_match(
    value: Optional[str], exact: bool, bind: Callable[[Parameter], str]
) -> Optional[str]:
    if value is None:
        return None

    if exact:
        return f"= {bind(value)}"

    if value.find(",") < 0:
        return f"LIKE {bind(value)}"

    items = [item.strip() for item in value.split(",")]
    return f"IN {bind(items)}"


def render_template(
    query: str,
    kwargs: Mapping[str, Any],
    get_version: Callable[[], str],
    bound: Optional[dict[str, Parameter]] = None,
) -> str:
    def bind(value: Parameter) -> str:
        if bound is None:
            if isinstance(value, Identifier):
                return quote_identifier(value)
            if isinstance(value, list):
                return "(" + ",".join(quote_string(item) for item in value) + ")"
            return quote_string(str(value))
        index = len(bound)
        while f"ch_tools_{index}" in bound:
            index += 1
        key = f"ch_tools_{index}"
        bound[key] = value
        if isinstance(value, Identifier):
            type_name = "Identifier"
        elif isinstance(value, list):
            type_name = "Array(String)"
        else:
            type_name = "String"
        return "{" + key + ":" + type_name + "}"

    env = Environment()
    env.filters.update(
        identifier=quote_identifier,
        literal=lambda value: quote_string(str(value)),
        bind=lambda value: bind(str(value)),
        bind_identifier=lambda value: bind(Identifier(value)),
    )

    env.globals["version_ge"] = lambda version: version_ge(get_version(), version)
    env.globals["format_str_match"] = lambda value, exact=False: _format_str_match(
        value, exact, bind
    )
    env.globals["format_str_imatch"] = lambda value: (
        _format_str_match(value.lower(), False, bind) if value is not None else None
    )

    template = env.from_string(query)
    return template.render(kwargs)
