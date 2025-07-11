import pytest

from ch_tools.common.clickhouse.config import ClickhouseConfig
from ch_tools.common.clickhouse.config.path import (
    CLICKHOUSE_SERVER_CONFIG_PATH,
    CLICKHOUSE_SERVER_PREPROCESSED_CONFIG_PATH,
)

# type: ignore


@pytest.mark.parametrize(
    "files,result",
    [
        pytest.param(
            {
                CLICKHOUSE_SERVER_CONFIG_PATH: """
                    <clickhouse>
                        <path>/var/lib/clickhouse/</path>
                        <tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
                    </clickhouse>
                    """,
            },
            {
                "clickhouse": {
                    "path": "/var/lib/clickhouse/",
                    "tmp_path": "/var/lib/clickhouse/tmp/",
                },
            },
            id="simple config",
        ),
        pytest.param(
            {
                CLICKHOUSE_SERVER_CONFIG_PATH: """
                    <clickhouse>
                        <path>/var/lib/clickhouse/</path>
                        <tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
                        <include_from>/etc/clickhouse-server/includes.xml</include_from>
                        <zookeeper incl="zookeeper-config"/>
                    </clickhouse>
                    """,
                "/etc/clickhouse-server/includes.xml": """
                    <clickhouse>
                        <zookeeper-config>
                            <node index="1">
                                <host>host1.yandex.net</host>
                                <port>2181</port>
                            </node>
                            <node index="2">
                                <host>host2.yandex.net</host>
                                <port>2181</port>
                            </node>
                            <node index="3">
                                <host>host3.yandex.net</host>
                                <port>2181</port>
                            </node>
                            <root>/clickhouse/cluster1</root>
                        </zookeeper-config>
                    </clickhouse>
                    """,
                "/etc/clickhouse-server/config.d/rabbitmq.xml": """
                    <clickhouse>
                        <rabbitmq>
                            <username>rabbitmq_user1</username>
                            <password>rabbitmq_password1</password>
                        </rabbitmq>
                    </clickhouse>
                    """,
                "/etc/clickhouse-server/config.d/empty.xml": """
                    <clickhouse/>
                    """,
            },
            {
                "clickhouse": {
                    "path": "/var/lib/clickhouse/",
                    "tmp_path": "/var/lib/clickhouse/tmp/",
                    "include_from": "/etc/clickhouse-server/includes.xml",
                    "rabbitmq": {
                        "username": "rabbitmq_user1",
                        "password": "*****",
                    },
                    "zookeeper": {
                        "node": [
                            {
                                "@index": "1",
                                "host": "host1.yandex.net",
                                "port": "2181",
                            },
                            {
                                "@index": "2",
                                "host": "host2.yandex.net",
                                "port": "2181",
                            },
                            {
                                "@index": "3",
                                "host": "host3.yandex.net",
                                "port": "2181",
                            },
                        ],
                        "root": "/clickhouse/cluster1",
                    },
                },
            },
            id="multi-file config",
        ),
        pytest.param(
            {
                CLICKHOUSE_SERVER_PREPROCESSED_CONFIG_PATH: """
                    <clickhouse>
                        <path>/var/lib/clickhouse/</path>
                        <tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
                    </clickhouse>
                    """,
            },
            {
                "clickhouse": {
                    "path": "/var/lib/clickhouse/",
                    "tmp_path": "/var/lib/clickhouse/tmp/",
                },
            },
            id="preprocessed config",
        ),
    ],
)
def test_config(fs, files, result):
    for file_path, contents in files.items():
        fs.create_file(file_path, contents=contents)

    config = ClickhouseConfig.load(try_preprocessed=True)
    assert config.dump() == result


@pytest.mark.parametrize(
    "files,result",
    [
        pytest.param(
            {
                CLICKHOUSE_SERVER_CONFIG_PATH: """
                    <clickhouse>
                        <zookeeper>
                            <node></node>
                            <root></root>
                        </zookeeper>
                    </clickhouse>
                    """,
            },
            False,
            id="with ZK config",
        ),
        pytest.param(
            {
                CLICKHOUSE_SERVER_CONFIG_PATH: """
                    <clickhouse>
                        <path>/var/lib/clickhouse/</path>
                    </clickhouse>
                    """,
            },
            True,
            id="without ZK config",
        ),
        pytest.param(
            {
                CLICKHOUSE_SERVER_CONFIG_PATH: """
                    <clickhouse>
                        <zookeeper/>
                    </clickhouse>
                    """,
            },
            True,
            id="with empty ZK config",
        ),
    ],
)
def test_config_zookeeper(fs, files, result):
    for file_path, contents in files.items():
        fs.create_file(file_path, contents=contents)

    assert ClickhouseConfig.load().zookeeper.is_empty() == result
