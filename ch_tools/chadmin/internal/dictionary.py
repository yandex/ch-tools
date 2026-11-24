from typing import Any, Optional

from click import Context

from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common.clickhouse.client.sql import qualified_name, quote_identifier


def list_dictionaries(
    ctx: Context, *, name: Optional[str] = None, status: Optional[str] = None
) -> Any:
    """
    List external dictionaries.
    """
    query = """
        SELECT
            database,
            name,
            status,
            type,
            source
        FROM system.dictionaries
        WHERE 1
        {% if name %}
          AND name = {{ name|literal }}
        {% endif %}
        {% if status %}
          AND status = {{ status|literal }}
        {% endif %}
        """
    return execute_query(ctx, query, name=name, status=status, format_="JSON")["data"]


def reload_dictionary(
    ctx: Context, *, name: str, database: Optional[str] = None
) -> None:
    """
    Reload external dictionary.
    """
    if database:
        full_name = qualified_name(database, name)
    else:
        full_name = quote_identifier(name)

    query = f"""SYSTEM RELOAD DICTIONARY {full_name}"""
    execute_query(ctx, query, format_=None)
