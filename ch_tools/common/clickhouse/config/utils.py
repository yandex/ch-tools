import os.path
from copy import deepcopy
from typing import Any, MutableMapping

import xmltodict
import yaml

from ch_tools.common.utils import first_value


def load_config(config_path: str, configd_dir: str = "config.d") -> Any:
    """
    Load ClickHouse config file.
    """
    # Load main config file.
    config = _load_config(config_path)

    # Load config files from config.d/ directory.
    configd_path = os.path.join(os.path.dirname(config_path), configd_dir)
    if os.path.exists(configd_path):
        for file in os.listdir(configd_path):
            file_path = os.path.join(configd_path, file)
            if file_path.endswith(".xml") or file_path.endswith(".yaml"):
                _merge_configs(config, _load_config(file_path))

    # Process includes.
    root_section = first_value(config)
    include_file = root_section.get("include_from")
    if include_file:
        include_config = first_value(_load_config(include_file))
        _apply_config_directives(root_section, include_config)

    return config


def dump_config(
    config: Any, *, mask_secrets: bool = True, xml_format: bool = False
) -> Any:
    """
    Dump ClickHouse config.
    """
    result = deepcopy(config)

    if mask_secrets:
        _mask_secrets(result)

    if xml_format:
        result = xmltodict.unparse(result, pretty=True)

    return result


def _load_config(config_path: str) -> Any:
    with open(config_path, "r", encoding="utf-8") as file:
        if config_path.endswith(".xml"):
            return xmltodict.parse(file.read(), disable_entities=False)
        return {"clickhouse": yaml.safe_load(file)}


def _merge_configs(main_config: Any, additional_config: Any) -> None:
    for key, value in additional_config.items():
        if key not in main_config:
            main_config[key] = value
            continue

        if isinstance(main_config[key], dict) and isinstance(value, dict):
            _merge_configs(main_config[key], value)
            continue

        if value is not None:
            main_config[key] = value


def _apply_config_directives(config_section: dict, include_config: dict) -> None:
    for key, item in config_section.items():
        if not isinstance(item, dict):
            continue

        include = item.get("@incl")
        if include:
            config_section[key] = include_config[include]
            continue

        _apply_config_directives(item, include_config)


def _mask_secrets(config: Any) -> None:
    if isinstance(config, MutableMapping):
        for key, value in list(config.items()):
            if isinstance(value, MutableMapping):
                _mask_secrets(config[key])
            elif key in ("password", "secret_access_key", "header", "identity"):
                config[key] = "*****"
