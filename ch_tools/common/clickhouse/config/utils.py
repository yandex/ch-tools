from copy import deepcopy
from typing import MutableMapping

import xmltodict


def _mask_secrets(config):
    if isinstance(config, MutableMapping):
        for key, value in list(config.items()):
            if isinstance(value, MutableMapping):
                _mask_secrets(config[key])
            elif key in ("password", "secret_access_key", "header", "identity"):
                config[key] = "*****"


def _load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as file:
        return xmltodict.parse(file.read())


def _dump_config(config, *, mask_secrets=True, xml_format=False):
    result = deepcopy(config)

    if mask_secrets:
        _mask_secrets(result)

    if xml_format:
        result = xmltodict.unparse(result, pretty=True)

    return result
