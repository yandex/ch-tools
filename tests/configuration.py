"""
Variables that influence testing behavior are defined here.
"""


def create():
    """
    Create test configuration (non-idempotent function).
    """
    network_name = "ch_tools_test"
    services: dict = {
        "clickhouse": {
            "instances": ["clickhouse01", "clickhouse02"],
            "expose": {
                "http": 8123,
                "clickhouse": 9000,
                "keeper": 2281,
            },
            "depends_on": ["zookeeper"],
            "args": {
                "CLICKHOUSE_VERSION": "${CLICKHOUSE_VERSION:-latest}",
            },
            "db": {
                "user": "reader",
                "password": "reader_password",
            },
        },
        "zookeeper": {
            "instances": ["zookeeper01"],
            "expose": {
                "tcp": 2181,
            },
        },
        "minio": {
            "instances": ["minio01"],
            "expose": {
                "http": 9000,
            },
        },
        "http_mock": {
            "instances": ["http_mock01"],
            "expose": {
                "tcp": 8080,
            },
        },
    }

    return {
        "images_dir": "images",
        "staging_dir": "staging",
        "network_name": network_name,
        "s3": {
            "endpoint": "http://minio01:9000",
            "access_secret_key": "test_secret",
            "access_key_id": "test_key",
            "bucket": "test",
        },
        "ch_backup": {
            "encrypt_key": "test_encrypt",
        },
        "services": services,
        "dbaas_conf": _dbaas_conf(services, network_name),
    }


def _dbaas_conf(services: dict, network_name: str) -> dict:
    """
    Generate dbaas.conf contents.
    """

    def _fqdn(instance_name):
        return f"{instance_name}.{network_name}"

    return {
        "cluster_id": "cid1",
        "created_at": "2022-01-01T12:00:00.000000+03:00",
        "cluster": {
            "subclusters": {
                "subcid1": {
                    "roles": ["clickhouse_cluster"],
                    "shards": {
                        "shard_id1": {
                            "name": "shard1",
                            "hosts": {
                                _fqdn(instance_name): {}
                                for instance_name in services["clickhouse"]["instances"]
                            },
                        },
                    },
                },
                "subcid2": {
                    "roles": ["zk"],
                    "hosts": {
                        _fqdn(services["zookeeper"]["instances"][0]): {},
                    },
                },
            },
        },
    }
