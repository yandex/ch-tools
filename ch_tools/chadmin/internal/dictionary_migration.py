from fnmatch import fnmatch
from glob import glob
from pathlib import Path
from typing import Any, Optional

from click import Context

from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.clickhouse.config.path import CLICKHOUSE_SERVER_CONFIG_PATH
from ch_tools.common.clickhouse.config.utils import load_config_file
from ch_tools.common.process_pool import WorkerTask, execute_tasks_in_parallel


def migrate_dictionaries(
    ctx: Context,
    dry_run: bool,
    should_remove: bool,
    target_database: str,
    include_pattern: Optional[str] = None,
    exclude_pattern: Optional[str] = None,
) -> None:
    """
    Migrate external dictionaries to DDL.
    """
    config_glob_pattern = _get_dictionary_config_paths_pattern()
    logging.debug("External dictionary config paths pattern: {}", config_glob_pattern)

    if Path(config_glob_pattern).is_absolute():
        config_path_list = [Path(p) for p in glob(config_glob_pattern)]
    else:
        config_path = Path(CLICKHOUSE_SERVER_CONFIG_PATH)
        config_directory = config_path.parent
        config_path_list = list(config_directory.glob(config_glob_pattern))

    if not config_path_list:
        logging.info(
            "No dictionary config files found matching pattern '{}", config_glob_pattern
        )
        return

    all_dictionaries: list[tuple[Path, str, str]] = []
    for config_file in config_path_list:
        if not _matches_patterns(config_file, include_pattern, exclude_pattern):
            continue
        queries = _generate_ddl_dictionaries_from_xml(str(config_file), target_database)
        for dict_name, query in queries:
            all_dictionaries.append((config_file, dict_name, query))

    if not all_dictionaries:
        logging.info(
            "No dictionary config files found matching include pattern '{}' and unmatching exclude pattern '{}'",
            include_pattern,
            exclude_pattern,
        )
        return

    if dry_run:
        _dry_run(all_dictionaries)
    else:
        _run(ctx, target_database, all_dictionaries, should_remove)


def _dry_run(filtered_dictionaries: list[tuple[Path, str, str]]) -> None:
    logging.info("Starting dry external dictionaries migration")
    for i, (config_file, dict_name, query) in enumerate(filtered_dictionaries, start=1):
        logging.info(f"config file '{config_file}' | dictionary name = '{dict_name}'")
        logging.info(f"query #{i}:\n{query}")

    logging.info(
        f"Total dictionaries that ready to migration: {len(filtered_dictionaries)}"
    )


def _run(
    ctx: Context,
    target_database: str,
    filtered_dictionaries: list[tuple[Path, str, str]],
    should_remove: bool,
    max_workers: int = 4,
) -> None:
    logging.info("Starting external dictionaries migration")
    logging.info("Creating '{}' database", target_database)
    execute_query(ctx, f"CREATE DATABASE IF NOT EXISTS {target_database}", format_=None)

    tasks = [
        WorkerTask(
            dict_name,
            _migrate_single_dictionary,
            {
                "ctx": ctx,
                "config_file": config_file,
                "dict_name": dict_name,
                "query": query,
            },
        )
        for config_file, dict_name, query in filtered_dictionaries
    ]

    results = execute_tasks_in_parallel(
        tasks, max_workers=max_workers, keep_going=False
    )
    logging.info("External dictionaries migration completed successfully")
    logging.info(f"Total dictionaries migrated: {len(results)}")

    if should_remove:
        _remove_dictionaries(filtered_dictionaries)


def _migrate_single_dictionary(
    ctx: Context, config_file: Path, dict_name: str, query: str
) -> bool:
    try:
        execute_query(ctx, query, format_=None)
        logging.info("Successfully migrated dictionary '{}'", dict_name)
        return True
    except Exception as e:
        raise RuntimeError(
            f"Dictionary migration failed for dictionary '{dict_name}' in config file '{config_file}'"
        ) from e


def _remove_dictionaries(filtered_dictionaries: list[tuple[Path, str, str]]) -> None:
    logging.info("Starting removing external dictionaries after migration")

    unique_files = {config_file for config_file, _, _ in filtered_dictionaries}

    for config_file in unique_files:
        try:
            config_file.unlink()
            logging.info(f"Deleted config file '{config_file}'")
        except Exception as e:
            raise RuntimeError(f"Error while removing '{config_file}'") from e
    logging.info("Removing external dictionaries completed successfully")


def _matches_patterns(
    config_file: Path,
    include_pattern: Optional[str],
    exclude_pattern: Optional[str],
) -> bool:
    config_file_str = str(config_file)
    if include_pattern:
        if not (
            fnmatch(config_file_str, include_pattern)
            or fnmatch(config_file.name, include_pattern)
        ):
            return False
    if exclude_pattern:
        if fnmatch(config_file_str, exclude_pattern) or fnmatch(
            config_file.name, exclude_pattern
        ):
            return False

    return True


def _get_dictionary_nodes_from_config(config: dict[str, Any]) -> list[dict[str, Any]]:
    dictionary_node_list = []

    dictionary_node = config.get("dictionary")
    if dictionary_node is not None:
        if isinstance(dictionary_node, list):
            return dictionary_node
        return [dictionary_node]

    for value in config.values():
        if isinstance(value, dict):
            dictionaries = _get_dictionary_nodes_from_config(value)
            dictionary_node_list.extend(dictionaries)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    dictionaries = _get_dictionary_nodes_from_config(item)
                    dictionary_node_list.extend(dictionaries)

    return dictionary_node_list


def _generate_ddl_dictionaries_from_xml(
    config_path: str, target_database: str
) -> list[tuple[str, str]]:
    """
    Parse XML dictionary config and generate CREATE DICTIONARY statements.
    """
    config: dict[str, Any] = load_config_file(config_path)
    dictionaries = _get_dictionary_nodes_from_config(config)

    return [
        _build_dictionary_ddl_from_config(attrs, target_database)
        for attrs in dictionaries
    ]


def _get_dictionary_config_paths_pattern() -> str:
    config_path = CLICKHOUSE_SERVER_CONFIG_PATH
    parsed_config = load_config_file(CLICKHOUSE_SERVER_CONFIG_PATH)

    clickhouse = parsed_config.get("clickhouse")
    if clickhouse is None:
        raise RuntimeError(
            f"Config '{config_path}' must contain <clickhouse> root element"
        )

    dictionaries_config = clickhouse.get("dictionaries_config")
    if not isinstance(dictionaries_config, str):
        raise RuntimeError(
            f"Config '{config_path}' must contain <clickhouse><dictionaries_config> element of type string"
        )

    return dictionaries_config


def _build_dictionary_ddl_from_config(
    attrs: dict[str, Any], target_database: str
) -> tuple[str, str]:
    name = _get_xml_dictionary_name(attrs)
    source = _build_xml_dictionary_source_block(attrs)
    lifetime = _build_xml_dictionary_lifetime_block(attrs)
    layout = _build_xml_dictionary_layout_block(attrs)
    primary_key, structure = _build_structure_and_primary_key(attrs)

    return name, _build_create_dictionary_statement(
        target_database, name, structure, primary_key, source, layout, lifetime, "", ""
    )


def _get_xml_dictionary_name(attrs: dict[str, Any]) -> str:
    name = attrs.get("name")
    if name is None or not isinstance(name, str):
        raise RuntimeError("Attribute <name> is missing or is not a string")
    return name


def _build_xml_dictionary_source_block(attrs: dict[str, Any]) -> str:
    source_name = attrs.get("source")
    if source_name is None or not isinstance(source_name, dict):
        raise RuntimeError("Missing <source> block in dictionary config")
    source_type = list(source_name.keys())[0]
    source_params = source_name[source_type]

    if not isinstance(source_params, dict):
        raise RuntimeError("Invalid <source> block in dictionary config")

    params: list[str] = []
    for param, value in source_params.items():
        if value is None:
            params.append(f"{param.upper()} ''")
            continue
        if not isinstance(value, str):
            raise RuntimeError(
                f"Value of <{param}> in <source><{source_type}> must be a string"
            )
        params.append(f"{param.upper()} {value}")

    str_params = " ".join(params)
    return f"SOURCE({source_type}({str_params}))"


def _format_layout_param_value(value: str) -> str:
    try:
        float(value)
        return value
    except ValueError:
        return f"'{value}'"


def _build_xml_dictionary_layout_block(attrs: dict[str, Any]) -> str:
    layout = attrs.get("layout")
    if layout is None:
        raise RuntimeError("Dictionary config must contain a <layout> block")
    if not isinstance(layout, dict) or len(layout) != 1:
        raise RuntimeError("<layout> element must contain exactly one layout type")

    layout_type = list(layout.keys())[0]
    layout_value = layout[layout_type]

    if layout_value is None:
        return f"LAYOUT({layout_type.upper()}())"
    if isinstance(layout_value, dict) and not layout_value:
        return f"LAYOUT({layout_type.upper()}())"

    if not isinstance(layout_value, dict):
        raise RuntimeError(
            f"Invalid <layout> block: expected dict or None, got {type(layout_value)}"
        )

    params: list[str] = []
    for param, value in layout_value.items():
        if not isinstance(value, str):
            raise RuntimeError(
                f"Value of <{param}> in <layout><{layout_type}> must be a string, got {type(value)}"
            )
        formated_value = _format_layout_param_value(value)
        params.append(f"{param.upper()} {formated_value}")

    str_params = " ".join(params)
    return f"LAYOUT({layout_type.upper()}({str_params}))"


def _build_xml_dictionary_lifetime_block(attrs: dict[str, Any]) -> str:
    lifetime = attrs.get("lifetime")
    if lifetime is None:
        return "LIFETIME(0)"
    if isinstance(lifetime, str):
        return f"LIFETIME({lifetime})"

    if not isinstance(lifetime, dict):
        raise RuntimeError("Dictionary config has invalid <lifetime> block")

    min_val = lifetime.get("min")
    if min_val == "" or min_val is None:
        min_val = None
    max_val = lifetime.get("max")
    if max_val == "" or max_val is None:
        max_val = None

    if min_val is None and max_val is None:
        return "LIFETIME(0)"

    if max_val is None:
        return f"LIFETIME({min_val})"

    return f"LIFETIME(MIN {min_val} MAX {max_val})"


def _build_structure_and_primary_key(attrs: dict[str, Any]) -> tuple[str, str]:
    structure = attrs.get("structure")
    if structure is None:
        raise RuntimeError("Dictionary config must contain a <structure> block")

    primary_key = ""
    attribute_list: list[str] = []
    attr_id = structure.get("id")
    key = structure.get("key")

    if (attr_id is None) == (key is None):
        raise RuntimeError(
            "<structure> must contain either <id> or <key>, but not both"
        )

    if attr_id is not None:
        if not isinstance(attr_id, dict):
            raise RuntimeError("<id> must be a dictionary")
        name = attr_id.get("name")
        if not name:
            raise RuntimeError("<id> must contain <name>")
        primary_key = f"PRIMARY KEY {name}"
        attribute_list.append(f"\t{name} UInt64")
    else:
        key_attrs = key.get("attribute", [])
        if isinstance(key_attrs, dict):
            key_attrs = [key_attrs]

        if not key_attrs:
            raise RuntimeError("<key> must contain at least one <attribute>")

        key_names: list[str] = []
        for attr in key_attrs:
            attr_str = _build_attribute_definition(attr, False)
            attribute_list.append(attr_str)
            key_names.append(attr.get("name"))

        primary_key = f"PRIMARY KEY {', '.join(key_names)}"

    attributes = structure.get("attribute", [])
    if isinstance(attributes, dict):
        attributes = [attributes]

    for attr in attributes:
        attribute_list.append(_build_attribute_definition(attr))

    return primary_key, ",\n\t".join(attribute_list)


def _normalize_null_default_value(attr_type: str, null_value: str) -> str:
    if null_value is None:
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
        result = defaults.get(attr_type)
        if result is None:
            raise RuntimeError(f"Type '{attr_type}' doesn't support a default value")
        return result
    if attr_type in ("String", "FixedString"):
        return f"'{null_value}'"
    return null_value


def _build_attribute_definition(
    attr: dict[str, Any], require_null_value: bool = True
) -> str:
    name = attr.get("name")
    attr_type = attr.get("type")

    if not name or not attr_type:
        raise RuntimeError("Each <attribute> must have both <name> and <type>")

    parts = [name, attr_type]

    if require_null_value and "null_value" not in attr:
        raise RuntimeError("<null_value> is required for dictionary attributes")

    if "null_value" in attr:
        null_value = _normalize_null_default_value(attr_type, attr["null_value"])
        parts.append(f"DEFAULT {null_value}")

    if "expression" in attr:
        parts.append(f"EXPRESSION {attr['expression']}")

    if "hierarchical" in attr and attr["hierarchical"] not in ("0", "false", ""):
        parts.append("HIERARCHICAL")

    if "injective" in attr and attr["injective"] not in ("0", "false", ""):
        parts.append("INJECTIVE")

    if "is_object_id" in attr and attr["is_object_id"] not in ("0", "false", ""):
        parts.append("IS_OBJECT_ID")

    return " ".join(parts)


def _build_create_dictionary_statement(
    target_database: str,
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
CREATE DICTIONARY IF NOT EXISTS {target_database}.{name}
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
