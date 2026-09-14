import os
from types import SimpleNamespace
from typing import Any, Generator, Optional
from unittest.mock import ANY, patch

import pytest
from click.testing import CliRunner
from kazoo.exceptions import NoNodeError, NotEmptyError

from ch_tools.chadmin.cli.zookeeper_group import zookeeper_group
from ch_tools.chadmin.internal.zookeeper import (
    _delete_candidates,
    _delete_sweep,
    _DeleteCounts,
    _DeleteProgress,
    _path_delete_size,
    _probe_subtree,
    delete_recursive,
)

PATH = "/clickhouse/task_queue/ddl/query/shards/replica1:9440,replica2:9440/executed"


@pytest.fixture(autouse=True)
def silence_logging() -> Generator[None, None, None]:
    with patch("ch_tools.chadmin.internal.zookeeper.logging"):
        yield


class FakeTransaction:
    def __init__(self, zk: "FakeZooKeeper") -> None:
        self.zk = zk
        self.paths: list[str] = []

    def delete(self, path: str) -> None:
        self.paths.append(path)

    def commit(self) -> list[Any]:
        self.zk.transaction_sizes.append(len(self.paths))
        self.zk.transaction_paths.append(list(self.paths))
        self.zk.before_transaction(self.paths)
        if self.zk.fail_transactions:
            self.zk.fail_transactions -= 1
            return [NotEmptyError() for _ in self.paths]
        staged_children = {
            path: set(children) for path, children in self.zk.children.items()
        }
        for path in self.paths:
            if path not in staged_children:
                return [NoNodeError() for _ in self.paths]
            if staged_children[path]:
                return [NotEmptyError() for _ in self.paths]

            parent = os.path.dirname(path)
            if parent in staged_children:
                staged_children[parent].discard(os.path.basename(path))
            del staged_children[path]

        for path in self.paths:
            self.zk.remove_leaf(path)
        return [True for _ in self.paths]


class FakeZooKeeper:
    def __init__(self, children: dict[str, set[str]]) -> None:
        self.children = {path: set(names) for path, names in children.items()}
        self.exists_calls: list[str] = []
        self.get_children_calls: list[str] = []
        self.delete_calls: list[str] = []
        self.transaction_sizes: list[int] = []
        self.transaction_paths: list[list[str]] = []
        self.fail_transactions = 0

    def before_transaction(self, paths: list[str]) -> None:
        pass

    def exists(self, path: str) -> Any:
        self.exists_calls.append(path)
        if path not in self.children:
            return None
        return SimpleNamespace(children_count=len(self.children[path]))

    def get_children(self, path: str) -> list[str]:
        self.get_children_calls.append(path)
        if path not in self.children:
            raise NoNodeError
        return sorted(self.children[path])

    def transaction(self) -> FakeTransaction:
        return FakeTransaction(self)

    def delete(self, path: str, recursive: bool = False) -> None:
        self.delete_calls.append(path)
        if path not in self.children:
            raise NoNodeError
        if self.children[path] and not recursive:
            raise NotEmptyError
        if recursive:
            for child in list(self.children[path]):
                self.delete(os.path.join(path, child), recursive=True)
        self.remove_leaf(path)

    def remove_leaf(self, path: str) -> None:
        parent = os.path.dirname(path)
        if parent in self.children:
            self.children[parent].discard(os.path.basename(path))
        del self.children[path]


def test_probe_returns_complete_child_first_small_tree() -> None:
    zk = FakeZooKeeper({"/root": {"leaf"}, "/root/leaf": set()})

    result = _probe_subtree(zk, "/root")

    assert result.postorder == ["/root/leaf", "/root"]
    assert zk.get_children_calls == ["/root", "/root/leaf"]


def test_probe_stops_when_delete_bytes_exceed_limit() -> None:
    zk = FakeZooKeeper({"/root": {"long-child-name"}, "/root/long-child-name": set()})

    with patch(
        "ch_tools.chadmin.internal.zookeeper." "RECURSIVE_DELETE_TRANSACTION_MAX_BYTES",
        40,
    ):
        result = _probe_subtree(zk, "/root")

    assert result.postorder is None
    assert zk.get_children_calls == ["/root"]


def test_delete_candidates_reports_successful_transaction() -> None:
    zk = FakeZooKeeper(
        {"/root": {"first", "second"}, "/root/first": set(), "/root/second": set()}
    )
    progress = _DeleteCounts()

    not_empty_indices = _delete_candidates(
        zk, ["/root/first", "/root/second"], progress
    )

    assert not_empty_indices == []
    assert progress == _DeleteCounts(deleted=2)
    assert zk.transaction_sizes == [2]


def test_delete_candidates_falls_back_to_individual_outcomes() -> None:
    zk = FakeZooKeeper(
        {
            "/root": {"branch", "leaf"},
            "/root/branch": {"grandchild"},
            "/root/branch/grandchild": set(),
            "/root/leaf": set(),
        }
    )
    progress = _DeleteCounts()

    not_empty_indices = _delete_candidates(zk, ["/root/leaf", "/root/branch"], progress)

    assert not_empty_indices == [1]
    assert progress == _DeleteCounts(deleted=1, not_empty=1)
    assert zk.transaction_sizes == [2]


def test_delete_candidates_preserves_counts_on_unexpected_failure() -> None:
    zk = FakeZooKeeper({"/root": set()})
    counts = _DeleteCounts()
    zk.fail_transactions = 1

    with (
        patch.object(zk, "delete", side_effect=[None, RuntimeError("connection lost")]),
        pytest.raises(RuntimeError, match="connection lost"),
    ):
        _delete_candidates(zk, ["/root/first", "/root/second"], counts)

    assert counts == _DeleteCounts(deleted=1)


def test_delete_candidates_reports_absent_member() -> None:
    zk = FakeZooKeeper({"/root": set()})
    progress = _DeleteCounts()

    not_empty_indices = _delete_candidates(zk, ["/root/missing"], progress)

    assert not_empty_indices == []
    assert progress == _DeleteCounts(already_absent=1)


def test_delete_candidates_skips_multi_for_oversized_singleton() -> None:
    zk = FakeZooKeeper({"/root": {"leaf"}, "/root/leaf": set()})
    progress = _DeleteCounts()

    with patch(
        "ch_tools.chadmin.internal.zookeeper." "RECURSIVE_DELETE_TRANSACTION_MAX_BYTES",
        1,
    ):
        not_empty_indices = _delete_candidates(zk, ["/root/leaf"], progress)

    assert not_empty_indices == []
    assert progress == _DeleteCounts(deleted=1)
    assert zk.transaction_sizes == []


def test_large_delete_does_not_read_each_leaf() -> None:
    leaf_names = {f"leaf-{index}" for index in range(10_001)}
    zk = FakeZooKeeper(
        {
            "/root": leaf_names,
            **{f"/root/{leaf}": set() for leaf in leaf_names},
        }
    )

    delete_recursive(zk, ["/root"])

    assert zk.children == {}
    assert zk.get_children_calls == ["/root"]
    assert max(zk.transaction_sizes) <= 1_000


def test_direct_child_count_skips_small_tree_probe() -> None:
    zk = FakeZooKeeper(
        {
            "/root": {"first", "second"},
            "/root/first": set(),
            "/root/second": set(),
        }
    )

    with (
        patch(
            "ch_tools.chadmin.internal.zookeeper."
            "RECURSIVE_DELETE_TRANSACTION_MAX_OPS",
            2,
        ),
        patch(
            "ch_tools.chadmin.internal.zookeeper._probe_subtree"
        ) as mock_probe_subtree,
    ):
        delete_recursive(zk, ["/root"])

    assert zk.children == {}
    mock_probe_subtree.assert_not_called()


def test_large_delete_descends_only_into_nonempty_candidates() -> None:
    leaf_names = {f"leaf-{index}" for index in range(10_001)}
    zk = FakeZooKeeper(
        {
            "/root": leaf_names | {"branch"},
            "/root/branch": {"grandchild"},
            "/root/branch/grandchild": set(),
            **{f"/root/{leaf}": set() for leaf in leaf_names},
        }
    )

    delete_recursive(zk, ["/root"])

    assert zk.children == {}
    assert zk.get_children_calls == ["/root", "/root/branch"]
    branch_attempts = [
        index
        for index, paths in enumerate(zk.transaction_paths)
        if "/root/branch" in paths
    ]
    grandchild_attempt = next(
        index
        for index, paths in enumerate(zk.transaction_paths)
        if "/root/branch/grandchild" in paths
    )
    assert len(branch_attempts) == 2
    assert branch_attempts[0] < grandchild_attempt < branch_attempts[1]


def test_large_delete_batches_are_parent_local_and_bounded() -> None:
    leaf_names = {f"leaf-{index}" for index in range(6)}
    zk = FakeZooKeeper(
        {
            "/root": leaf_names,
            **{f"/root/{leaf}": set() for leaf in leaf_names},
        }
    )

    with (
        patch(
            "ch_tools.chadmin.internal.zookeeper."
            "RECURSIVE_DELETE_TRANSACTION_MAX_OPS",
            2,
        ),
        patch(
            "ch_tools.chadmin.internal.zookeeper."
            "RECURSIVE_DELETE_TRANSACTION_MAX_BYTES",
            100,
        ),
    ):
        delete_recursive(zk, ["/root"])

    assert zk.children == {}
    assert all(len(paths) <= 2 for paths in zk.transaction_paths)
    assert all(
        len({os.path.dirname(path) for path in paths}) == 1
        for paths in zk.transaction_paths
    )
    assert all(
        sum(_path_delete_size(path) for path in paths) <= 100
        for paths in zk.transaction_paths
    )


def test_bounded_scan_does_not_queue_a_large_nested_directory() -> None:
    zk = FakeZooKeeper(
        {
            "/root": {"branch"},
            "/root/branch": {"leaf-0", "leaf-1", "leaf-2"},
            "/root/branch/leaf-0": set(),
            "/root/branch/leaf-1": set(),
            "/root/branch/leaf-2": set(),
        }
    )

    with (
        patch(
            "ch_tools.chadmin.internal.zookeeper."
            "RECURSIVE_DELETE_TRANSACTION_MAX_OPS",
            3,
        ),
    ):
        delete_recursive(zk, ["/root"])

    assert zk.children == {}
    assert zk.get_children_calls == [
        "/root",
        "/root/branch",
        "/root",
        "/root/branch",
    ]


class RacingZooKeeper(FakeZooKeeper):
    def __init__(self, children: dict[str, set[str]], continuous: bool) -> None:
        super().__init__(children)
        self.continuous = continuous
        self.created = 0

    def before_transaction(self, paths: list[str]) -> None:
        should_race = paths == ["/root"] and not self.children["/root"]
        if should_race and (self.continuous or self.created == 0):
            child = f"late-{self.created}"
            self.created += 1
            self.children["/root"].add(child)
            self.children[f"/root/{child}"] = set()


def test_large_delete_retries_a_finite_creation_race() -> None:
    leaf_names = {f"leaf-{index}" for index in range(10_001)}
    zk = RacingZooKeeper(
        {
            "/root": leaf_names,
            **{f"/root/{leaf}": set() for leaf in leaf_names},
        },
        continuous=False,
    )

    delete_recursive(zk, ["/root"])

    assert zk.children == {}
    assert zk.created == 1
    assert zk.get_children_calls == ["/root", "/root"]


def test_large_delete_aborts_after_three_sweeps_with_continuous_writer() -> None:
    leaf_names = {f"leaf-{index}" for index in range(10_001)}
    zk = RacingZooKeeper(
        {
            "/root": leaf_names,
            **{f"/root/{leaf}": set() for leaf in leaf_names},
        },
        continuous=True,
    )

    with pytest.raises(RuntimeError, match="after 3 sweeps"):
        delete_recursive(zk, ["/root"], max_sweeps=3)

    assert zk.created == 3
    assert zk.get_children_calls == ["/root", "/root", "/root"]


def test_small_delete_does_not_select_large_algorithm() -> None:
    zk = FakeZooKeeper(
        {
            "/root": {"leaf"},
            "/root/leaf": set(),
        }
    )

    delete_recursive(zk, ["/root"])

    assert zk.children == {}
    assert zk.get_children_calls == ["/root", "/root/leaf"]
    assert zk.transaction_paths == [["/root/leaf", "/root"]]


def test_failed_small_transaction_rereads_children_in_large_mode() -> None:
    zk = FakeZooKeeper({"/root": {"leaf"}, "/root/leaf": set()})
    zk.fail_transactions = 1

    delete_recursive(zk, ["/root"])

    assert zk.children == {}
    assert zk.get_children_calls == ["/root", "/root/leaf", "/root"]


def test_failed_atomic_leaf_rereads_children_in_large_mode() -> None:
    zk = FakeZooKeeper({"/root": set()})
    zk.fail_transactions = 1

    delete_recursive(zk, ["/root"])

    assert zk.children == {}
    assert zk.get_children_calls == ["/root", "/root"]


def test_delete_timeout_stops_before_next_batch_and_checks_root() -> None:
    zk = FakeZooKeeper(
        {
            "/root": {"branch", "leaf"},
            "/root/branch": {"grandchild"},
            "/root/branch/grandchild": set(),
            "/root/leaf": set(),
        }
    )

    with (
        patch(
            "ch_tools.chadmin.internal.zookeeper."
            "RECURSIVE_DELETE_TRANSACTION_MAX_OPS",
            2,
        ),
        patch(
            "ch_tools.chadmin.internal.zookeeper.monotonic",
            side_effect=[0.0, 0.0, 0.0, 0.0, 2.0],
        ),
        pytest.raises(RuntimeError, match="deadline"),
    ):
        delete_recursive(zk, ["/root"], max_sweeps=0, delete_timeout=1.0)

    assert "/root/leaf" not in zk.children
    assert "/root/branch" in zk.children
    assert zk.get_children_calls == ["/root"]
    assert zk.transaction_paths == [["/root/branch", "/root/leaf"]]
    assert zk.exists_calls[-1] == "/root"


def test_sweep_accumulates_counts_across_batches() -> None:
    zk = FakeZooKeeper(
        {
            "/root": {"first", "second", "third"},
            "/root/first": set(),
            "/root/second": set(),
            "/root/third": set(),
        }
    )
    progress = _DeleteProgress()
    with patch(
        "ch_tools.chadmin.internal.zookeeper.RECURSIVE_DELETE_TRANSACTION_MAX_OPS", 2
    ):
        retained, deadline_reached = _delete_sweep(zk, "/root", progress)

    assert zk.children == {}
    assert progress.counts == _DeleteCounts(deleted=4)
    assert retained == 0
    assert not deadline_reached
    assert zk.transaction_sizes == [2, 1, 1]


def test_small_dry_run_visits_tree_without_deleting() -> None:
    zk = FakeZooKeeper({"/root": {"leaf"}, "/root/leaf": set()})

    delete_recursive(zk, ["/root"], dry_run=True)

    assert zk.children == {"/root": {"leaf"}, "/root/leaf": set()}
    assert zk.transaction_paths == []
    assert zk.delete_calls == []
    assert zk.get_children_calls == ["/root", "/root/leaf"]


def test_dry_run_visits_all_nodes_beyond_preview_limit() -> None:
    zk = FakeZooKeeper(
        {
            "/root": {"private-leaf-0", "private-leaf-1", "private-leaf-2"},
            "/root/private-leaf-0": set(),
            "/root/private-leaf-1": set(),
            "/root/private-leaf-2": set(),
        }
    )

    with (
        patch(
            "ch_tools.chadmin.internal.zookeeper."
            "RECURSIVE_DELETE_DRY_RUN_MAX_LISTED_PATHS",
            2,
        ),
    ):
        delete_recursive(zk, ["/root"], dry_run=True)

    assert len(zk.children) == 4
    assert zk.transaction_paths == []
    assert zk.delete_calls == []
    assert set(zk.get_children_calls) == set(zk.children)
    assert len(zk.get_children_calls) == len(zk.children)


class UnconfirmedDeletionZooKeeper(FakeZooKeeper):
    def __init__(self, children: dict[str, set[str]]) -> None:
        super().__init__(children)
        self.root_deleted = False

    def exists(self, path: str) -> Any:
        if path == "/root" and self.root_deleted:
            return SimpleNamespace(children_count=0)
        return super().exists(path)

    def remove_leaf(self, path: str) -> None:
        super().remove_leaf(path)
        if path == "/root":
            self.root_deleted = True


def test_delete_reports_error_if_root_absence_is_not_confirmed() -> None:
    zk = UnconfirmedDeletionZooKeeper({"/root": set()})

    with pytest.raises(RuntimeError, match="root still exists"):
        delete_recursive(zk, ["/root"])


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
            {"max_sweeps": 3, "delete_timeout": None},
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


def test_delete_command_forwards_sweep_and_deadline_options() -> None:
    with patch(
        "ch_tools.chadmin.cli.zookeeper_group.delete_zk_nodes"
    ) as mock_delete_zk_nodes:
        result = CliRunner().invoke(
            zookeeper_group,
            [
                "delete",
                "--max-sweeps",
                "0",
                "--delete-timeout",
                "12.5",
                PATH,
            ],
            obj={"config": {"loguru": {"handlers": {}}}},
        )

    assert result.exit_code == 0, result.output
    mock_delete_zk_nodes.assert_called_once_with(
        ANY, [PATH], max_sweeps=0, delete_timeout=12.5
    )


@pytest.mark.parametrize(
    "option",
    [
        pytest.param(["--max-sweeps", "-1"], id="negative-sweeps"),
        pytest.param(["--delete-timeout", "0"], id="zero-timeout"),
        pytest.param(["--delete-timeout", "-1"], id="negative-timeout"),
    ],
)
def test_delete_command_rejects_invalid_limits(option: list[str]) -> None:
    result = CliRunner().invoke(
        zookeeper_group,
        ["delete", *option, PATH],
        obj={"config": {"loguru": {"handlers": {}}}},
    )

    assert result.exit_code != 0
