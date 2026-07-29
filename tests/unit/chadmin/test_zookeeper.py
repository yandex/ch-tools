from unittest.mock import ANY, patch

from click.testing import CliRunner

from ch_tools.chadmin.cli.zookeeper_group import zookeeper_group


def test_create_command_preserves_commas_in_path() -> None:
    path = (
        "/clickhouse/task_queue/ddl/query/shards/replica1:9440,replica2:9440/executed"
    )

    with patch(
        "ch_tools.chadmin.cli.zookeeper_group.create_zk_nodes"
    ) as mock_create_zk_nodes:
        result = CliRunner().invoke(
            zookeeper_group,
            ["create", path],
            obj={"config": {"loguru": {"handlers": {}}}},
        )

    assert result.exit_code == 0, result.output
    mock_create_zk_nodes.assert_called_once_with(ANY, [path], None, make_parents=False)
