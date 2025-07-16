import os
import sys
from collections import OrderedDict
from typing import Any, Optional

import yaml
import yaml.representer


def dict_representer(dumper: Any, data: Any) -> Any:
    return yaml.representer.SafeRepresenter.represent_dict(dumper, data.items())


def str_representer(dumper: Any, data: Any) -> Any:
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


def load_yaml(file_path: str) -> Any:
    with open(os.path.expanduser(file_path), "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def dump_yaml(data: Any, file_path: Optional[str] = None) -> Any:
    if not file_path:
        return yaml.dump(
            data, default_flow_style=False, allow_unicode=True, width=sys.maxsize
        )

    with open(os.path.expanduser(file_path), "w", encoding="utf-8") as f:
        return yaml.dump(
            data, f, default_flow_style=False, allow_unicode=True, width=sys.maxsize
        )
