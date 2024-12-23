import os.path
import socket
from copy import deepcopy

from ch_tools.common.utils import deep_merge
from ch_tools.common.yaml import load_yaml

CHADMIN_LOG_FILE = "/var/log/chadmin/chadmin.log"
CH_MONITORING_LOG_FILE = "/var/log/clickhouse-monitoring/clickhouse-monitoring.log"
KEEPER_MONITORING_LOG_FILE = "/var/log/keeper-monitoring/keeper-monitoring.log"

CONFIG_FILE = "/etc/clickhouse-tools/config.yaml"

S3_LOG_CONFIG = {
    "sink": f"{CHADMIN_LOG_FILE}",
    "level": "WARNING",
    "format": "default",
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
        "attach_table_timeout": 10 * 60,
        "detach_table_timeout": 10 * 60,
        "alter_table_timeout": 10 * 60,
        "drop_table_timeout": 10 * 60,
        "unfreeze_timeout": 10 * 60,
        "restart_replica_timeout": 10 * 60,
        "restore_replica_timeout": 10 * 60,
        "drop_replica_timeout": 10 * 60,
    },
    "object_storage": {
        "bucket_name_prefix": "cloud-storage-",
        "clean": {
            "listing_table_prefix": "listing_objects_from_",
            "listing_table_database": "default",
            "storage_policy": "default",
            "antijoin_timeout": 10 * 60,
        },
    },
    "zookeeper": {
        "randomize_hosts": True,
        "username": None,
        "password": None,
    },
    # Configuration of chadmin tool commands and options.
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
        "zookeeper": {
            "clean_zk_metadata_for_hosts": {
                "workers": 10,
                # In the wrost case 10 min * 25 about 4 h.
                "retry_min_wait_sec": 60,
                "retry_max_wait_sec": 60 * 10,
                "max_retries": 25,
            },
        },
    },
    # Monitoring settings. It applies to the both ch-monitoring and keeper-monitoring tools.
    "monitoring": {
        "output": {
            "escaping_rules": [
                {
                    "pattern": "\n",
                    "replacement": " ",
                },
                {
                    "pattern": r"(Code:\s\d+\.\sDB::Exception:\s).*(\s\([A-Z_]*\)\s\(version\s.*\s\(official build\)\)).*",
                    "replacement": r"\1...\2",
                },
            ],
        },
    },
    # Configuration of ch-monitoring tool commands and options.
    "ch-monitoring": {
        "log-errors": {
            "@disabled": False,
            "crit": 60,
            "warn": 6,
            "watch_seconds": 600,
            "exclude": r"e\.displayText\(\) = No message received",
            "logfile": "/var/log/clickhouse-server/clickhouse-server.err.log",
        },
        "core-dumps": {
            "@disabled": False,
            "core_directory": "/var/cores/",
            "crit_seconds": 60 * 10,
        },
        "replication-lag": {
            "@disabled": False,
            "xcrit": 3600,
            "crit": 600,
            "warn": 300,
            "mcrit": 90.0,
            "mwarn": 50.0,
        },
        "system-queues": {
            "@disabled": False,
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
        "geobase": {
            "@disabled": False,
        },
        "dns": {
            "@disabled": False,
            "ipv4": True,
            "ipv6": False,
            "cluster": False,
            "private": False,
            "imdsv2": False,
        },
        "tls": {
            "crit": 10,
            "warn": 30,
        },
    },
    # Configuration of keeper-monitoring tool commands and options.
    "keeper-monitoring": {
        "tls": {
            "crit": 10,
            "warn": 30,
        },
    },
    # Logging configuration.
    "loguru": {
        "formatters": {
            "default": "{time:YYYY-MM-DD HH:mm:ss,SSS} {process.name:11} {process.id:5} [{level:8}] {extra[logger_name]} {extra[cmd_name]}: {message}",
        },
        "handlers": {
            "chadmin": {
                "chadmin": {
                    "sink": CHADMIN_LOG_FILE,
                    "level": "DEBUG",
                    "format": "default",
                },
                "boto3": S3_LOG_CONFIG,
                "botocore": S3_LOG_CONFIG,
                "nose": S3_LOG_CONFIG,
                "s3transfer": S3_LOG_CONFIG,
                "urllib3": S3_LOG_CONFIG,
                "kazoo": S3_LOG_CONFIG,
            },
            "ch-monitoring": {
                "ch-monitoring": {
                    "sink": CH_MONITORING_LOG_FILE,
                    "level": "DEBUG",
                    "format": "default",
                },
                "urllib3.connectionpool": {
                    "sink": CH_MONITORING_LOG_FILE,
                    "level": "CRITICAL",
                    "format": "default",
                },
            },
            "keeper-monitoring": {
                "keeper-monitoring": {
                    "sink": KEEPER_MONITORING_LOG_FILE,
                    "level": "DEBUG",
                    "format": "default",
                },
            },
        },
    },
    # Settings specific for cloud deployments.
    "cloud": {
        "metadata_service_endpoint": "http://169.254.169.254",
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
