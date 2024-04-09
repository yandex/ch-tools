import os.path
import socket
from copy import deepcopy

from ch_tools.common.utils import deep_merge
from ch_tools.common.yaml import load_yaml

CONFIG_FILE = "/etc/clickhouse-tools/config.yaml"
DEFAULT_CONFIG = {
    "clickhouse": {
        "host": socket.getfqdn(),
        "protocol": "https",
        "insecure": False,
        "port": 8443,
        "user": None,
        "password": None,
        "settings": {},
        "monitoring_user": None,
        "monitoring_password": None,
        "distributed_ddl_path": "/clickhouse/task_queue/ddl",
        "timeout": 60,
        "alter_table_timeout": 600,
    },
    "object_storage": {
        "clean": {
            "listing_table_prefix": "listing_objects_from_",
            "listing_table_database": "default",
            "storage_policy": "default",
        }
    },
    "zookeeper": {
        "randomize_hosts": True,
    },
}


def load_config():
    """
    Read config file, apply defaults and return result configuration.
    """
    config = deepcopy(DEFAULT_CONFIG)

    if os.path.exists(CONFIG_FILE):
        loaded_config = load_yaml(CONFIG_FILE)
        deep_merge(config, loaded_config)

    return config
