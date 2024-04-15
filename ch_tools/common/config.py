import os.path
import socket
from copy import deepcopy

from ch_tools.common.utils import deep_merge
from ch_tools.common.yaml import load_yaml

CHADMIN_LOG_FILE = "/var/log/chadmin/chadmin.log"
CH_MONITORING_LOG_FILE = "/var/log/clickhouse-monitoring/clickhouse-monitoring.log"

CONFIG_FILE = "/etc/clickhouse-tools/config.yaml"

S3_LOG_CONFIG = {
    "sink": f"{CHADMIN_LOG_FILE}",
    "level": "WARNING",
    "format": "ch-tools",
}

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
    "chadmin": {
        "wait": {
            "replication-sync": {
                "replica_timeout": "1h",
                "total_timeout": "3d",
                "status": 0,
                "pause": "30s",
                "xcrit": 3600,
                "crit": 600,
                "warn": 300,
                "mcrit": 90.0,
                "mwarn": 50.0,
            },
        },
    },
    "ch-monitoring": {
        "log-errors": {
            "crit": 60,
            "warn": 6,
            "watch_seconds": 600,
            "exclude": r"e\.displayText\(\) = No message received",
            "logfile": "/var/log/clickhouse-server/clickhouse-server.err.log",
        },
        "core-dumps": {
            "core_directory": "/var/cores/",
            "crit_seconds": 60 * 10,
        },
        "replication-lag": {
            "xcrit": 3600,
            "crit": 600,
            "warn": 300,
            "mcrit": 90.0,
            "mwarn": 50.0,
        },
        "system-queues": {
            "merges_in_queue_warn": 10,
            "merges_in_queue_crit": 20,
            "future_parts_warn": 10,
            "future_parts_crit": 20,
            "parts_to_check_warn": 10,
            "parts_to_check_crit": 20,
            "queue_size_warn": 10,
            "queue_size_crit": 20,
            "inserts_in_queue_warn": 10,
            "inserts_in_queue_crit": 20,
        },
    },
    "keeper-monitoring": {},
    "loguru": {
        "formatters": {
            "ch-tools": "{time:YYYY-MM-DD HH:mm:ss,SSS} {process.name:11} {process.id:5} [{level:8}] {extra[logger_name]}: {message}",
        },
        "handlers": {
            "chadmin": {
                "chadmin": {
                    "sink": CHADMIN_LOG_FILE,
                    "level": "DEBUG",
                    "format": "ch-tools",
                },
                "boto3": S3_LOG_CONFIG,
                "botocore": S3_LOG_CONFIG,
                "nose": S3_LOG_CONFIG,
                "s3transfer": S3_LOG_CONFIG,
                "urllib3": S3_LOG_CONFIG,
            },
            "ch-monitoring": {
                "ch-monitoring": {
                    "sink": CH_MONITORING_LOG_FILE,
                    "level": "DEBUG",
                    "format": "ch-tools",
                },
                "urllib3.connectionpool": {
                    "sink": CH_MONITORING_LOG_FILE,
                    "level": "CRITICAL",
                    "format": "ch-tools",
                },
            },
        },
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
