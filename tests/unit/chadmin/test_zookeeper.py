from typing import Any, Optional
from unittest.mock import ANY, patch

import pytest
from click.testing import CliRunner

from ch_tools.chadmin.cli.zookeeper_group import zookeeper_group

PATH = "/clickhouse/task_queue/ddl/query/shards/replica1:9440,replica2:9440/executed"


@pytest.mark.parametrize(
    "args,value,make_parents",
    [
        pytest.param(["create", PATH], None, False, id="no-value"),
        pytest.param(
            ["create", "--make-parents", PATH, "value"],
            "value",
            True,
            id="value-and-make-parents",
        ),
    ],
)
def test_create_command_forwards_path_value_and_make_parents(
    args: list[str], value: Optional[str], make_parents: bool
) -> None:
    with patch(
        "ch_tools.chadmin.cli.zookeeper_group.create_zk_nodes"
    ) as mock_create_zk_nodes:
        result = CliRunner().invoke(
            zookeeper_group,
            args,
            obj={"config": {"loguru": {"handlers": {}}}},
        )

    assert result.exit_code == 0, result.output
    mock_create_zk_nodes.assert_called_once_with(
        ANY, [PATH], value, make_parents=make_parents
    )


PATHS = [PATH, "/clickhouse/task_queue/ddl/query/shards/replica3/executed"]


@pytest.mark.parametrize(
    "args,command,command_args,command_kwargs",
    [
        pytest.param(
            [
                "create",
                "--path",
                PATHS[0],
                "--path",
                PATHS[1],
                "--value",
                "value",
                "--make-parents",
            ],
            "create_zk_nodes",
            (PATHS, "value"),
            {"make_parents": True},
            id="create",
        ),
        pytest.param(
            [
                "update",
                "--path",
                PATHS[0],
                "--path",
                PATHS[1],
                "--value",
                "value",
            ],
            "update_zk_nodes",
            (PATHS, "value"),
            {},
            id="update",
        ),
        pytest.param(
            ["delete", "--path", PATHS[0], "--path", PATHS[1]],
            "delete_zk_nodes",
            (PATHS,),
            {},
            id="delete",
        ),
    ],
)
def test_commands_support_repeated_path_options(
    args: list[str],
    command: str,
    command_args: tuple[Any, ...],
    command_kwargs: dict[str, Any],
) -> None:
    with patch(f"ch_tools.chadmin.cli.zookeeper_group.{command}") as mock_command:
        result = CliRunner().invoke(
            zookeeper_group,
            args,
            obj={"config": {"loguru": {"handlers": {}}}},
        )

    assert result.exit_code == 0, result.output
    mock_command.assert_called_once_with(ANY, *command_args, **command_kwargs)


def test_delete_command_rejects_multiple_paths() -> None:
    result = CliRunner().invoke(
        zookeeper_group,
        ["delete", PATH, "/clickhouse/task_queue/ddl/query/shards/replica3/executed"],
        obj={"config": {"loguru": {"handlers": {}}}},
    )

    assert result.exit_code != 0
    assert "Got unexpected extra argument" in result.output
