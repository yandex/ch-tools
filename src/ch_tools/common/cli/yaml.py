import os
import sys
from collections import OrderedDict

import yaml
import yaml.representer


def dict_representer(dumper, data):
    return yaml.representer.SafeRepresenter.represent_dict(dumper, data.items())


def str_representer(dumper, data):
    if "\n" in data:
        style = "|"
    else:
        style = None

    return yaml.representer.SafeRepresenter.represent_scalar(
        dumper, "tag:yaml.org,2002:str", data, style=style
    )


yaml.add_representer(dict, dict_representer)
yaml.add_representer(OrderedDict, dict_representer)
yaml.add_representer(str, str_representer)


def load_yaml(file_path):
    with open(os.path.expanduser(file_path), "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def dump_yaml(data, file_path=None):
    if not file_path:
        return yaml.dump(
            data, default_flow_style=False, allow_unicode=True, width=sys.maxsize
        )

    with open(os.path.expanduser(file_path), "w", encoding="utf-8") as f:
        yaml.dump(
            data, f, default_flow_style=False, allow_unicode=True, width=sys.maxsize
        )
