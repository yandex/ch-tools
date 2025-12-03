from pathlib import Path
from typing import Any, Optional

from click import Context

from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.clickhouse.config.path import CLICKHOUSE_SERVER_CONFIG_PATH
from ch_tools.common.clickhouse.config.utils import _load_config


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
          AND name = '{{ name }}'
        {% endif %}
        {% if status %}
          AND status = '{{ status }}'
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
        full_name = f"`{database}`.`{name}`"
    else:
        full_name = f"`{name}`"

    query = f"""SYSTEM RELOAD DICTIONARY {full_name}"""
    execute_query(ctx, query, format_=None)


def migrate_dictionaries(ctx: Context) -> None:
    """Migrate external dictionaries to DDL."""
    logging.info("Creating '_dictionaries' database")
    execute_query(ctx, "CREATE DATABASE IF NOT EXISTS _dictionaries", format_=None)

    config_glob_pattern = _get_dictionaries_config_pattern()
    logging.debug("External dictionary pattern: {}", config_glob_pattern)

    config_path = Path(CLICKHOUSE_SERVER_CONFIG_PATH)
    config_directory = config_path.parent

    logging.info("Starting external dictionaries migration")
    for i, config_file in enumerate(
        config_directory.glob(config_glob_pattern), start=1
    ):
        logging.info("Migration #{}: dictionary config file '{}'", i, config_file)
        try:
            queries = generate_ddl_dictionary_from_xml(str(config_file))
            for j, query in enumerate(queries, start=1):
                logging.debug("Query #{}:\n{}", j, query)
                execute_query(ctx, query, format_=None)
        except Exception as error:
            logging.exception(
                "Failed to migrate dictionary from config file '{}'", config_file
            )
            logging.critical("Error message: {}", error)
            raise RuntimeError(
                f"Dictionary migration failed for config file '{config_file}'"
            ) from error
    logging.info("External dictionaries migration completed successfully")


def generate_ddl_dictionary_from_xml(config_path: str) -> list[str]:
    """
    Parse XML dictionary config and generate CREATE DICTIONARY statements.
    """
    config: dict[str, Any] = _load_config(config_path)

    queries: list[str] = []
    dictionaries = config.get("dictionaries")

    if dictionaries is None:
        raise RuntimeError("Dictionary config is missing <dictionaries> section")

    dictionary_nodes = dictionaries.get("dictionary")
    if dictionary_nodes is None:
        raise RuntimeError(
            "Dictionary config is missing <dictionaries><dictionary> section"
        )

    if not isinstance(dictionary_nodes, list) or isinstance(dictionary_nodes, dict):
        dictionary_nodes = [dictionary_nodes]

    for attrs in dictionary_nodes:
        if isinstance(attrs, dict):
            queries.append(_build_dictionary_ddl_from_config(attrs))

    return queries


def _get_dictionaries_config_pattern() -> str:
    config_path = CLICKHOUSE_SERVER_CONFIG_PATH
    parsed_config = _load_config(CLICKHOUSE_SERVER_CONFIG_PATH)

    clickhouse = parsed_config.get("clickhouse", None)
    if clickhouse is None:
        raise RuntimeError(
            f"Config '{config_path}' must contain <clickhouse> root element"
        )

    dictionaries_config = clickhouse.get("dictionaries_config", None)
    if dictionaries_config is None or not isinstance(dictionaries_config, str):
        raise RuntimeError(
            f"Config '{config_path}' must contain <clickhouse><dictionaries_config> element of type string"
        )

    return dictionaries_config


def _build_dictionary_ddl_from_config(attrs: dict[str, Any]) -> str:
    name = _get_dictionary_name(attrs)
    source = _build_source_block(attrs)
    lifetime = _build_lifetime_block(attrs)
    layout = _build_layout_block(attrs)
    primary_key, structure = _build_structure_and_primary_key(attrs)

    query = _build_create_dictionary_statement(
        name, structure, primary_key, source, layout, lifetime, "", ""
    )

    return query


def _get_dictionary_name(attrs: dict[str, Any]) -> str:
    name = attrs.get("name")
    if name is None or not isinstance(name, str):
        raise RuntimeError("Attribute <name> is missing or is not a string")
    return name


def _build_source_block(attrs: dict[str, Any]) -> str:
    source_name = attrs.get("source")
    if source_name is None or not isinstance(source_name, dict):
        raise RuntimeError("Missing <source> block in dictionary config")
    source_type = list(source_name.keys())[0]
    source_params = source_name[source_type]

    if not isinstance(source_params, dict):
        raise RuntimeError("Invalid <source> block in dictionary config")

    params: list[str] = []
    for param, value in source_params.items():
        if not isinstance(value, str):
            raise RuntimeError(
                f"Value of <{param}> in <source><{source_type}> must be a string"
            )

        params.append(f"{param.upper()} {value}")

    str_params = " ".join(params)
    return f"SOURCE({source_type}({str_params}))"


def _build_layout_block(attrs: dict[str, Any]) -> str:
    layout = attrs.get("layout")
    if layout is None:
        raise RuntimeError(
            "<layout> element must contain exactly one layout definition"
        )
    if len(layout) != 1:
        raise RuntimeError("<layout> element must contain exactly one child element")

    layout_type = list(layout.keys())[0]
    if isinstance(layout_type, str):
        return f"LAYOUT({layout_type.upper()}())"

    layout_params = layout[layout_type]
    if not isinstance(layout_params, dict):
        raise RuntimeError("Invalid <layout> block in dictionary config")

    params: list[str] = []
    for param, value in layout_params.items():
        if not isinstance(value, str):
            raise RuntimeError(f"Value of attribute in <{layout_params}> must be str")
        params.append(f"[{param.upper()} {value.upper()}]")

    str_params = " ".join(params)
    return f"LAYOUT({layout_params}({str_params}))"


def _build_lifetime_block(attrs: dict[str, Any]) -> str:
    lifetime = attrs.get("lifetime", None)
    if lifetime is None:
        return "LIFETIME(0)"
    if isinstance(lifetime, (str, int)):
        return f"LIFETIME({lifetime})"

    if not isinstance(lifetime, dict):
        raise RuntimeError("Dictionary config has invalid <lifetime> block")

    min_val = lifetime.get("min", None)
    max_val = lifetime.get("max", None)

    if min_val is None and max_val is None:
        raise RuntimeError(
            "At least one of <min> or <max> must be specified in <lifetime>"
        )

    if max_val is None and not isinstance(min_val, (str, int)):
        return f"LIFETIME(({min_val}))"

    if not isinstance(min_val, (str, int)) or not isinstance(max_val, (str, int)):
        raise RuntimeError("<max> and <min> must each contain a single value")

    return f"LIFETIME(MIN {min_val} MAX {max_val})"


def _build_structure_and_primary_key(attrs: dict[str, Any]) -> tuple[str, str]:
    structure = attrs.get("structure")
    if structure is None:
        raise RuntimeError("Dictionary config must contain a <structure> block")

    primary_key = ""
    attribute_list: list[str] = []
    attr_id = structure.get("id")
    key = structure.get("key")

    attributes = structure.get("attribute", [])

    if (attr_id is None) == (key is None):
        raise RuntimeError(
            "<structure> must contain either <id> or <key>, but not both"
        )

    if attr_id is not None:
        name = attr_id.get("name")
        primary_key = f"PRIMARY KEY {name}"
        attribute_list.append(f"\t{name} UInt64")
    else:
        key_attrs = key.get("attribute", [])
        if isinstance(key_attrs, dict):
            key_attrs = [key_attrs]

        key_names: list[str] = []
        for attr in key_attrs:
            attr_str = _build_attribute_definition(attr)
            attribute_list.append(attr_str)
            key_names.append(attr.get("name"))

        primary_key = f"PRIMARY KEY {', '.join(key_names)}"

    if isinstance(attributes, dict):
        attributes = [attributes]

    for attr in attributes:
        attribute_list.append(_build_attribute_definition(attr))

    return primary_key, ",\n\t".join(attribute_list)


def _normalize_null_default_value(attr_type: str, null_value: str) -> str:
    defaults = {
        "Int8": "0",
        "Int16": "0",
        "Int32": "0",
        "Int64": "0",
        "Int128": "0",
        "Int256": "0",
        "UInt8": "0",
        "UInt16": "0",
        "UInt32": "0",
        "UInt64": "0",
        "UInt128": "0",
        "UInt256": "0",
        "Float32": "0",
        "Float64": "0",
        "Decimal": "0",
        "Decimal32": "0",
        "Decimal64": "0",
        "Decimal128": "0",
        "Decimal256": "0",
        "String": "''",
        "FixedString": "''",
        "Date": "1970-01-01",
        "Date32": "1900-01-01",
        "DateTime": "1970-01-01 00:00:00",
        "DateTime64": "1970-01-01 00:00:00.000",
        "Bool": "0",
        "UUID": "00000000-0000-0000-0000-000000000000",
        "IPv4": "0.0.0.0",
        "IPv6": "::",
        "Array": "[]",
        "Tuple": "()",
        "Map": "{}",
        "Nullable": "NULL",
        "Point": "(0, 0)",
        "Ring": "[]",
        "Polygon": "[]",
        "MultiPolygon": "[]",
    }
    if null_value is None:
        return defaults[attr_type]
    if attr_type in ("String", "FixedString"):
        return f"'{null_value}'"
    return null_value


def _build_attribute_definition(attr: dict[str, Any]) -> str:
    name = attr.get("name")
    attr_type = attr.get("type")

    if not name or not attr_type:
        raise RuntimeError("Each <attribute> must have both <name> and <type>")

    parts = [name, attr_type]

    if "null_value" in attr:
        null_value = _normalize_null_default_value(attr_type, attr["null_value"])
        parts.append(f"DEFAULT {null_value}")

    if attr.get("expression"):
        parts.append(f"EXPRESSION {attr['expression']}")

    if attr.get("hierarchical"):
        parts.append("HIERARCHICAL")

    if attr.get("injective"):
        parts.append("INJECTIVE")

    if attr.get("is_object_id"):
        parts.append("IS_OBJECT_ID")

    return " ".join(parts)


def _build_create_dictionary_statement(
    name: str,
    structure: str,
    primary_key: str,
    source: str,
    layout: str,
    lifetime: str,
    settings: str,
    comment: str,
) -> str:
    return f"""
CREATE DICTIONARY IF NOT EXISTS _dictionaries.{name}
(
    {structure}
)
{primary_key}
{source}
{layout}
{lifetime}
{settings}
{comment}
           """
