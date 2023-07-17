from ch_tools.chadmin.internal.utils import execute_query


def list_dictionaries(ctx, *, name=None, status=None):
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
          AND name = '{{ name }}'
        {% endif %}
        {% if status %}
          AND status = '{{ status }}'
        {% endif %}
        """
    return execute_query(ctx, query, name=name, status=status, format_="JSON")["data"]


def reload_dictionary(ctx, *, name, database=None):
    """
    Reload external dictionary.
    """
    if database:
        full_name = f"`{database}`.`{name}`"
    else:
        full_name = f"`{name}`"

    query = f"""SYSTEM RELOAD DICTIONARY {full_name}"""
    execute_query(ctx, query, format_=None)
