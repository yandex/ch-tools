from cloud.mdb.clickhouse.tools.chadmin.internal.utils import execute_query


def get_dictionaries(ctx, *, name=None, status=None):
    """
    Get dictionaries.
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
    return execute_query(ctx, query, name=name, status=status, format='JSON')['data']


def reload_dictionary(ctx, *, name, database=None):
    """
    Reload dictionary.
    """
    if database:
        full_name = f'`{database}`.`{name}`'
    else:
        full_name = f'`{name}`'

    query = f"""SYSTEM RELOAD DICTIONARY {full_name}"""
    execute_query(ctx, query, format=None)
