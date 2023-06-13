"""
Variables that influence testing behavior are defined here.
"""
import os
import random

from modules.utils import generate_random_string


def create():
    """
    Create test configuration (non-idempotent function).
    """
    network_suffix = random.randint(0, 4096)
    network_name = f'test_net_{network_suffix}'

    version_parts = os.getenv("CLICKHOUSE_VERSION", "0.0").split('.')
    assert len(version_parts) >= 2, "Invalid version string"
    maj_ver, min_ver = int(version_parts[0]), int(version_parts[1])
    keeper_supported = maj_ver > 21 or (maj_ver == 21 and min_ver >= 8)

    keeper_port = 2183

    keeper_tcp_port = 2181  # or 2281
    keeper_secure = False

    services: dict = {
        'clickhouse': {
            'instances': ['clickhouse01', 'clickhouse02'],
            'expose': {
                'http': 8123,
                'clickhouse': 9000,
                'ssh': 22,
                'keeper': keeper_port,
            },
            'depends_on': ['zookeeper'],
            'args': {
                'CLICKHOUSE_VERSION': '$CLICKHOUSE_VERSION',
                'NETWORK_NAME': network_name,
            },
            'db': {
                'user': 'reader',
                'password': 'reader_password',
            },
            'keeper': {
                'enabled': keeper_supported,
                'port': keeper_tcp_port,
                'secure': keeper_secure
            },
        },
        'zookeeper': {
            'secure': keeper_secure,
            'instances': ['zookeeper01'],
            'port': keeper_tcp_port,
            'expose': {'tcp': keeper_tcp_port},
        },
        'minio': {
            'instances': ['minio01'],
            'expose': {
                'http': 9000,
            },
            'prebuild_cmd': [
                'mkdir -p images/minio/bin',
                '/usr/bin/s3cmd -c /etc/s3cmd.cfg get --skip-existing '
                's3://dbaas-infra-test-cache/minio.RELEASE.2021-01-16T02-19-44Z.gz '
                'bin/minio.gz',
                'gunzip -f bin/minio.gz',
                '/usr/bin/s3cmd -c /etc/s3cmd.cfg get --skip-existing '
                's3://dbaas-infra-test-cache/mc.RELEASE.2021-01-16T02-45-34Z.gz '
                'bin/mc.gz',
                'gunzip -f bin/mc.gz',
            ],
        },
        'http_mock': {
            'instances': ['http_mock01'],
            'expose': {
                'tcp': 8080,
            },
        },
    }

    return {
        'images_dir': 'images',
        'staging_dir': 'staging',
        'network_name': network_name,
        's3': {
            'endpoint': 'http://minio01:9000',
            'access_secret_key': generate_random_string(40),
            'access_key_id': generate_random_string(20),
            'bucket': 'test',
        },
        'ch_backup': {
            'encrypt_key': generate_random_string(32),
        },
        'services': services,
        'base_images': {
            'ch-tools-tests-base': {
                'tag': 'ch-tools-tests-base',
                'path': 'images/base',
            },
        },
        'dbaas_conf': _dbaas_conf(services, network_name),
    }


def _dbaas_conf(services: dict, network_name: str) -> dict:
    """
    Generate dbaas.conf contents.
    """

    def _fqdn(instance_name):
        return f'{instance_name}.{network_name}'

    return {
        'cluster_id': 'cid1',
        'created_at': '2022-01-01T12:00:00.000000+03:00',
        'cluster': {
            'subclusters': {
                'subcid1': {
                    'roles': ['clickhouse_cluster'],
                    'shards': {
                        'shard_id1': {
                            'name': 'shard1',
                            'hosts': {
                                _fqdn(instance_name): {} for instance_name in services['clickhouse']['instances']
                            },
                        },
                    },
                },
                'subcid2': {
                    'roles': ['zk'],
                    'hosts': {
                        _fqdn(services['zookeeper']['instances'][0]): {},
                    },
                },
            },
        },
    }
