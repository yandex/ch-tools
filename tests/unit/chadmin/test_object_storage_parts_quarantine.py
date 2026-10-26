import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Generator
from unittest.mock import ANY, MagicMock

import pytest
from click.testing import CliRunner
from kazoo.exceptions import NoNodeError

import ch_tools.chadmin.cli.data_store_group as data_store_group_module
import ch_tools.chadmin.internal.system as system_module
from ch_tools.chadmin.cli.data_store_group import data_store_group
from ch_tools.chadmin.internal.object_storage import parts_quarantine
from ch_tools.chadmin.internal.object_storage.parts_quarantine import (
    DETACHED_PREFIX,
    OfflineTable,
    QuarantineReport,
    bind_parts_to_tables,
    collect_zookeeper_part_nodes,
    discover_object_storage_parts,
    ensure_broken_detached_parts_are_not_cleaned,
    ensure_clickhouse_server_stopped,
    ensure_nonlocal_zookeeper_replicas_inactive,
    load_offline_tables,
    plan_replicas_to_mark_lost,
    preflight_local_moves,
    quarantine_object_storage_parts,
)
from ch_tools.chadmin.internal.system import SYSTEMCTL_PATH

TABLE_UUID = "a99cbcd7-af44-43fe-bb67-591133b8e6c7"
TABLE_ZK_PATH = f"/clickhouse/tables/01/{TABLE_UUID}"


class FakeZooKeeper:
    def __init__(self, children: dict[str, set[str]]) -> None:
        self.children = {path: set(value) for path, value in children.items()}
        self.values: dict[str, bytes] = {
            path: b"0" for path in children if path.endswith("/is_lost")
        }
        self.deleted: list[str] = []
        self.updated: list[tuple[str, bytes]] = []

    def exists(self, path: str) -> object | None:
        return SimpleNamespace() if path in self.children else None

    def get_children(self, path: str) -> list[str]:
        if path not in self.children:
            raise NoNodeError
        return sorted(self.children[path])

    def get(self, path: str) -> tuple[bytes, object]:
        if path not in self.children:
            raise NoNodeError
        return self.values.get(path, b""), SimpleNamespace()

    def set(self, path: str, value: bytes) -> None:
        if path not in self.children:
            raise NoNodeError
        self.values[path] = value
        self.updated.append((path, value))

    def delete(self, path: str) -> None:
        if path not in self.children:
            raise NoNodeError
        parent = str(Path(path).parent)
        if parent in self.children:
            self.children[parent].discard(Path(path).name)
        del self.children[path]
        self.deleted.append(path)


def make_part(table_path: Path, name: str, markers: bool = True) -> Path:
    part_path = table_path / name
    part_path.mkdir(parents=True)
    if markers:
        (part_path / "checksums.txt").touch()
        (part_path / "columns.txt").touch()
    return part_path


def make_store(tmp_path: Path) -> tuple[Path, Path]:
    store = tmp_path / "object_storage" / "store"
    table_path = store / TABLE_UUID[:3] / TABLE_UUID
    table_path.mkdir(parents=True)
    (table_path / "detached").mkdir()
    return store, table_path


def make_metadata(tmp_path: Path) -> Path:
    metadata = tmp_path / "metadata"
    database_path = metadata / "test"
    database_path.mkdir(parents=True)
    (metadata / "test.sql").write_text(
        "ATTACH DATABASE test\nENGINE = Atomic\n", encoding="utf-8"
    )
    (database_path / "table.sql").write_text(
        "\n".join(
            [
                f"ATTACH TABLE test.table UUID '{TABLE_UUID}'",
                "(",
                "    `id` UInt64",
                ")",
                "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{uuid}', '{replica}')",
                "ORDER BY id",
            ]
        ),
        encoding="utf-8",
    )
    return metadata


def make_zookeeper(part_names: set[str]) -> FakeZooKeeper:
    replicas_path = f"{TABLE_ZK_PATH}/replicas"
    children: dict[str, set[str]] = {replicas_path: {"r1", "r2"}}
    for replica in ("r1", "r2"):
        replica_path = f"{replicas_path}/{replica}"
        parts_path = f"{replica_path}/parts"
        is_lost_path = f"{replica_path}/is_lost"
        children[replica_path] = {"parts", "is_lost"}
        children[is_lost_path] = set()
        children[parts_path] = set(part_names)
        for part_name in part_names:
            children[f"{parts_path}/{part_name}"] = set()
    return FakeZooKeeper(children)


@pytest.fixture(autouse=True)
def silence_logging(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    logging_mock = MagicMock()
    monkeypatch.setattr(parts_quarantine, "logging", logging_mock)
    monkeypatch.setattr(
        parts_quarantine, "ensure_clickhouse_server_stopped", MagicMock()
    )
    monkeypatch.setattr(
        parts_quarantine,
        "get_clickhouse_config",
        lambda _ctx: SimpleNamespace(
            merge_tree_settings={},
            default_replica_path=None,
            default_replica_name=None,
        ),
    )
    monkeypatch.setattr(
        parts_quarantine,
        "get_macros",
        lambda _ctx: {"shard": "01", "replica": "r1"},
    )

    def delete_part_nodes(zk: FakeZooKeeper, nodes: list[str]) -> None:
        for node in nodes:
            try:
                zk.delete(node)
            except NoNodeError:
                pass

    monkeypatch.setattr(parts_quarantine, "delete_nodes_batched", delete_part_nodes)
    return logging_mock


def test_discovers_parts_tmp_directories_and_previous_run(tmp_path: Path) -> None:
    store, table_path = make_store(tmp_path)
    make_part(table_path, "all_1_1_0")
    make_part(table_path, "tmp_merge_all_1_2_1", markers=False)
    make_part(table_path, "all_2_2_0", markers=False)
    make_part(table_path, "not-a-part", markers=True)
    (table_path / "format_version.txt").touch()

    detached = table_path / "detached"
    make_part(detached, f"{DETACHED_PREFIX}all_3_3_0")
    make_part(detached, f"{DETACHED_PREFIX}all_5_5_0_try1")
    make_part(detached, "ignored_all_4_4_0")

    parts = discover_object_storage_parts(store)

    assert [(part.part_name, part.already_detached) for part in parts] == [
        ("all_1_1_0", False),
        ("tmp_merge_all_1_2_1", False),
        ("all_3_3_0", True),
        ("all_5_5_0", True),
    ]


def test_maps_table_uuid_to_expanded_zookeeper_path(tmp_path: Path) -> None:
    metadata = make_metadata(tmp_path)

    tables = load_offline_tables(metadata, {TABLE_UUID}, {"shard": "01"})

    assert tables[TABLE_UUID].database == "test"
    assert tables[TABLE_UUID].table == "table"
    assert tables[TABLE_UUID].zookeeper_path == TABLE_ZK_PATH


def test_uses_replicated_database_replica_for_table_macro(tmp_path: Path) -> None:
    metadata = make_metadata(tmp_path)
    (metadata / "test.sql").write_text(
        "ATTACH DATABASE test\n"
        "ENGINE = Replicated('/databases/test', 'db-shard', 'db-r1')\n",
        encoding="utf-8",
    )

    tables = load_offline_tables(
        metadata,
        {TABLE_UUID},
        {"shard": "server-shard", "replica": "wrong-server-replica"},
        require_replica_name=True,
    )

    assert tables[TABLE_UUID].replica_name == "db-r1"
    assert tables[TABLE_UUID].zookeeper_path == (
        f"/clickhouse/tables/db-shard/{TABLE_UUID}"
    )


def test_same_keeper_root_preserves_locals_and_collects_all_part_nodes() -> None:
    part_name = "all_1_1_0"
    zk = make_zookeeper({part_name})
    replicas_path = f"{TABLE_ZK_PATH}/replicas"
    zk.children[replicas_path].add("local-r3")
    zk.children[f"{replicas_path}/local-r3"] = {"parts", "is_lost"}
    zk.children[f"{replicas_path}/local-r3/parts"] = {part_name}
    zk.children[f"{replicas_path}/local-r3/parts/{part_name}"] = set()
    zk.children[f"{replicas_path}/local-r3/is_lost"] = set()
    zk.values[f"{replicas_path}/local-r3/is_lost"] = b"0"
    tables = {
        "uuid-r1": OfflineTable("db", "first", "uuid-r1", TABLE_ZK_PATH, "r1"),
        "uuid-r3": OfflineTable("db", "second", "uuid-r3", TABLE_ZK_PATH, "local-r3"),
    }

    plan = plan_replicas_to_mark_lost(zk, tables, mark_replicas_lost=True)

    assert plan.local_replicas == ["local-r3", "r1"]
    assert plan.replicas_to_mark_lost == ["r2"]
    assert plan.nodes_to_mark_lost == [f"{replicas_path}/r2/is_lost"]

    parts = [
        parts_quarantine.QuarantinedPart(
            table_uuid=table.uuid,
            part_name=part_name,
            source_path=Path(f"/store/{table.uuid}/{part_name}"),
            detached_path=Path(f"/store/{table.uuid}/detached/{part_name}"),
            temporary=False,
            already_detached=False,
            table=table,
        )
        for table in tables.values()
    ]
    expected_nodes = [
        f"{replicas_path}/local-r3/parts/{part_name}",
        f"{replicas_path}/r1/parts/{part_name}",
        f"{replicas_path}/r2/parts/{part_name}",
    ]

    assert collect_zookeeper_part_nodes(zk, parts) == expected_nodes
    assert all(part.zookeeper_nodes == expected_nodes for part in parts)


def test_fails_if_table_uuid_cannot_be_mapped(tmp_path: Path) -> None:
    metadata = tmp_path / "metadata"
    metadata.mkdir()

    with pytest.raises(ValueError, match="Cannot map object-storage table UUIDs"):
        load_offline_tables(metadata, {TABLE_UUID}, {})


def test_collects_exact_part_nodes_from_all_replicas(tmp_path: Path) -> None:
    store, table_path = make_store(tmp_path)
    make_part(table_path, "all_1_1_0")
    make_part(table_path, "tmp_merge_all_1_2_1", markers=False)
    metadata = make_metadata(tmp_path)
    parts = discover_object_storage_parts(store)
    tables = load_offline_tables(metadata, {TABLE_UUID}, {"shard": "01"})
    bind_parts_to_tables(parts, tables)
    zk = make_zookeeper({"all_1_1_0", "unrelated_2_2_0"})

    nodes = collect_zookeeper_part_nodes(zk, parts)

    assert nodes == [
        f"{TABLE_ZK_PATH}/replicas/r1/parts/all_1_1_0",
        f"{TABLE_ZK_PATH}/replicas/r2/parts/all_1_1_0",
    ]
    assert next(part for part in parts if part.temporary).zookeeper_nodes == []


def test_preflight_rejects_active_and_already_detached_copy(tmp_path: Path) -> None:
    store, table_path = make_store(tmp_path)
    make_part(table_path, "all_1_1_0")
    make_part(table_path / "detached", f"{DETACHED_PREFIX}all_1_1_0")
    parts = discover_object_storage_parts(store)

    with pytest.raises(ValueError, match="Both active and detached copies exist"):
        preflight_local_moves(parts)


def test_keep_going_uses_retry_for_active_and_already_detached_copy(
    tmp_path: Path,
) -> None:
    store, table_path = make_store(tmp_path)
    make_part(table_path, "all_1_1_0")
    make_part(table_path / "detached", f"{DETACHED_PREFIX}all_1_1_0")
    make_part(table_path, "all_2_2_0")
    parts = discover_object_storage_parts(store)

    selected = preflight_local_moves(parts, keep_going=True)

    assert [(part.part_name, part.detached_path.name) for part in selected] == [
        ("all_1_1_0", f"{DETACHED_PREFIX}all_1_1_0_try1"),
        ("all_2_2_0", f"{DETACHED_PREFIX}all_2_2_0"),
    ]


def test_keep_going_uses_retry_for_existing_destination(tmp_path: Path) -> None:
    store, table_path = make_store(tmp_path)
    source = make_part(table_path, "all_1_1_0")
    destination = table_path / "detached" / f"{DETACHED_PREFIX}all_1_1_0"
    destination.mkdir()
    parts = discover_object_storage_parts(store)

    selected = preflight_local_moves(parts, keep_going=True)

    assert len(selected) == 1
    assert selected[0].detached_path.name == f"{DETACHED_PREFIX}all_1_1_0_try1"
    assert source.is_dir()


def test_keep_going_uses_first_free_retry_suffix(tmp_path: Path) -> None:
    store, table_path = make_store(tmp_path)
    make_part(table_path, "all_1_1_0")
    destination = table_path / "detached" / f"{DETACHED_PREFIX}all_1_1_0"
    destination.mkdir()
    Path(f"{destination}_try1").mkdir()

    selected = preflight_local_moves(
        discover_object_storage_parts(store), keep_going=True
    )

    assert selected[0].detached_path.name == f"{DETACHED_PREFIX}all_1_1_0_try2"


@pytest.mark.parametrize("detached_is_part", [False, True])
def test_keep_going_moves_conflicting_part_and_deletes_its_keeper_node(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    detached_is_part: bool,
) -> None:
    store, table_path = make_store(tmp_path)
    conflicting_source = make_part(table_path, "all_1_1_0")
    conflicting_destination = make_part(
        table_path / "detached",
        f"{DETACHED_PREFIX}all_1_1_0",
        markers=detached_is_part,
    )
    movable_source = make_part(table_path, "all_2_2_0")
    metadata = make_metadata(tmp_path)
    zk = make_zookeeper({"all_1_1_0", "all_2_2_0"})

    @contextmanager
    def fake_zk_client(_ctx: object) -> Generator[FakeZooKeeper, None, None]:
        yield zk

    monkeypatch.setattr(parts_quarantine, "zk_client", fake_zk_client)

    report = quarantine_object_storage_parts(
        MagicMock(),
        str(store),
        str(metadata),
        None,
        dry_run=False,
        keep_going=True,
    )

    assert not conflicting_source.exists()
    assert conflicting_destination.is_dir()
    assert (table_path / "detached" / f"{DETACHED_PREFIX}all_1_1_0_try1").is_dir()
    assert not movable_source.exists()
    assert [part.part_name for part in report.parts] == ["all_1_1_0", "all_2_2_0"]
    assert any("all_1_1_0" in node for node in zk.deleted)
    for replica in ("r1", "r2"):
        assert (
            "all_1_1_0" not in zk.children[f"{TABLE_ZK_PATH}/replicas/{replica}/parts"]
        )


def test_dry_run_saves_manifest_without_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    silence_logging: MagicMock,
) -> None:
    store, table_path = make_store(tmp_path)
    source = make_part(table_path, "all_1_1_0")
    metadata = make_metadata(tmp_path)
    parts_file = tmp_path / "parts.json"
    zk = make_zookeeper({"all_1_1_0"})

    monkeypatch.setattr(
        parts_quarantine,
        "get_macros",
        lambda _ctx: {"shard": "01", "replica": "r1"},
    )

    @contextmanager
    def fake_zk_client(_ctx: object) -> Generator[FakeZooKeeper, None, None]:
        yield zk

    monkeypatch.setattr(parts_quarantine, "zk_client", fake_zk_client)

    report = quarantine_object_storage_parts(
        MagicMock(), str(store), str(metadata), str(parts_file), dry_run=True
    )

    assert source.is_dir()
    assert not zk.deleted
    assert not zk.updated
    silence_logging.info.assert_called_once_with(
        "Will move {} -> {}",
        source,
        table_path / "detached" / f"{DETACHED_PREFIX}all_1_1_0",
    )
    assert report.summary() == {
        "parts": 1,
        "moved": 0,
        "already_detached": 0,
        "zookeeper_nodes": 2,
        "local_replicas": ["r1"],
        "replicas_to_mark_lost": ["r2"],
        "dry_run": True,
    }
    manifest = json.loads(parts_file.read_text(encoding="utf-8"))
    assert manifest["dry_run"] is True
    assert manifest["parts"][0]["part_name"] == "all_1_1_0"
    assert manifest["parts"][0]["zookeeper_nodes"] == [
        f"{TABLE_ZK_PATH}/replicas/r1/parts/all_1_1_0",
        f"{TABLE_ZK_PATH}/replicas/r2/parts/all_1_1_0",
    ]


def test_existing_parts_file_blocks_operation_before_local_move(
    tmp_path: Path,
) -> None:
    store, table_path = make_store(tmp_path)
    source = make_part(table_path, "all_1_1_0")
    metadata = make_metadata(tmp_path)
    parts_file = tmp_path / "parts.json"
    parts_file.write_text("existing\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Parts file already exists"):
        quarantine_object_storage_parts(
            MagicMock(), str(store), str(metadata), str(parts_file), dry_run=False
        )

    assert source.is_dir()
    assert parts_file.read_text(encoding="utf-8") == "existing\n"


def test_moves_parts_and_deletes_part_nodes_from_every_replica(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, table_path = make_store(tmp_path)
    source = make_part(table_path, "all_1_1_0")
    temporary = make_part(table_path, "tmp_merge_all_1_2_1", markers=False)
    metadata = make_metadata(tmp_path)
    parts_file = tmp_path / "parts.json"
    zk = make_zookeeper({"all_1_1_0", "unrelated_2_2_0"})

    monkeypatch.setattr(
        parts_quarantine,
        "get_macros",
        lambda _ctx: {"shard": "01", "replica": "r1"},
    )

    @contextmanager
    def fake_zk_client(_ctx: object) -> Generator[FakeZooKeeper, None, None]:
        yield zk

    monkeypatch.setattr(parts_quarantine, "zk_client", fake_zk_client)

    report = quarantine_object_storage_parts(
        MagicMock(), str(store), str(metadata), str(parts_file), dry_run=False
    )

    assert not source.exists()
    assert not temporary.exists()
    assert (table_path / "detached" / f"{DETACHED_PREFIX}all_1_1_0").is_dir()
    assert (table_path / "detached" / f"{DETACHED_PREFIX}tmp_merge_all_1_2_1").is_dir()
    assert zk.deleted == [
        f"{TABLE_ZK_PATH}/replicas/r1/parts/all_1_1_0",
        f"{TABLE_ZK_PATH}/replicas/r2/parts/all_1_1_0",
    ]
    assert zk.updated == [
        (f"{TABLE_ZK_PATH}/replicas/r2/is_lost", b"1"),
    ]
    assert zk.values[f"{TABLE_ZK_PATH}/replicas/r1/is_lost"] == b"0"
    assert zk.values[f"{TABLE_ZK_PATH}/replicas/r2/is_lost"] == b"1"
    assert "unrelated_2_2_0" in zk.children[f"{TABLE_ZK_PATH}/replicas/r1/parts"]
    assert report.summary() == {
        "parts": 2,
        "moved": 2,
        "already_detached": 0,
        "zookeeper_nodes": 2,
        "local_replicas": ["r1"],
        "replicas_to_mark_lost": ["r2"],
        "dry_run": False,
    }
    assert json.loads(parts_file.read_text(encoding="utf-8"))["dry_run"] is False


def test_no_mark_replicas_lost_skips_identity_error_and_keeper_updates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, table_path = make_store(tmp_path)
    source = make_part(table_path, "all_1_1_0")
    metadata = make_metadata(tmp_path)
    zk = make_zookeeper({"all_1_1_0"})
    monkeypatch.setattr(parts_quarantine, "get_macros", lambda _ctx: {"shard": "01"})

    @contextmanager
    def fake_zk_client(_ctx: object) -> Generator[FakeZooKeeper, None, None]:
        yield zk

    monkeypatch.setattr(parts_quarantine, "zk_client", fake_zk_client)

    report = quarantine_object_storage_parts(
        MagicMock(),
        str(store),
        str(metadata),
        None,
        dry_run=False,
        mark_replicas_lost=False,
    )

    assert not source.exists()
    assert len(zk.deleted) == 2
    assert not zk.updated
    assert report.local_replicas == []
    assert report.replicas_to_mark_lost == []


def test_replica_identity_error_is_fatal_even_with_keep_going(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, table_path = make_store(tmp_path)
    source = make_part(table_path, "all_1_1_0")
    metadata = make_metadata(tmp_path)
    zk = make_zookeeper({"all_1_1_0"})
    monkeypatch.setattr(parts_quarantine, "get_macros", lambda _ctx: {"shard": "01"})

    @contextmanager
    def fake_zk_client(_ctx: object) -> Generator[FakeZooKeeper, None, None]:
        yield zk

    monkeypatch.setattr(parts_quarantine, "zk_client", fake_zk_client)

    with pytest.raises(
        parts_quarantine.ReplicaIdentityError,
        match="--no-mark-replicas-lost",
    ):
        quarantine_object_storage_parts(
            MagicMock(),
            str(store),
            str(metadata),
            None,
            dry_run=False,
            keep_going=True,
        )

    assert source.is_dir()
    assert not zk.deleted
    assert not zk.updated


def test_second_run_cleans_keeper_for_already_detached_part(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, table_path = make_store(tmp_path)
    make_part(table_path / "detached", f"{DETACHED_PREFIX}all_1_1_0")
    metadata = make_metadata(tmp_path)
    zk = make_zookeeper({"all_1_1_0"})

    monkeypatch.setattr(
        parts_quarantine,
        "get_macros",
        lambda _ctx: {"shard": "01", "replica": "r1"},
    )

    @contextmanager
    def fake_zk_client(_ctx: object) -> Generator[FakeZooKeeper, None, None]:
        yield zk

    monkeypatch.setattr(parts_quarantine, "zk_client", fake_zk_client)

    report = quarantine_object_storage_parts(
        MagicMock(), str(store), str(metadata), None, dry_run=False
    )

    assert report.moved == 0
    assert report.already_detached == 1
    assert len(zk.deleted) == 2


def test_fails_when_detached_directory_is_missing(tmp_path: Path) -> None:
    store, table_path = make_store(tmp_path)
    (table_path / "detached").rmdir()
    make_part(table_path, "all_1_1_0")

    with pytest.raises(ValueError, match="Detached directory does not exist"):
        discover_object_storage_parts(store)


def test_custom_detached_prefix_is_used_and_discovered(tmp_path: Path) -> None:
    store, table_path = make_store(tmp_path)
    make_part(table_path, "all_1_1_0")

    parts = discover_object_storage_parts(store, "s3-quarantine_")

    assert parts[0].detached_path.name == "s3-quarantine_all_1_1_0"


@pytest.mark.parametrize(
    "engine",
    ["ReplicatedMergeTree()", "ReplicatedReplacingMergeTree(version)"],
)
def test_default_replica_path_is_used_when_engine_omits_replica_arguments(
    tmp_path: Path, engine: str
) -> None:
    metadata = tmp_path / "metadata"
    database_path = metadata / "test"
    database_path.mkdir(parents=True)
    (metadata / "test.sql").write_text(
        "ATTACH DATABASE test\nENGINE = Atomic\n", encoding="utf-8"
    )
    (database_path / "table.sql").write_text(
        "\n".join(
            [
                f"ATTACH TABLE test.table UUID '{TABLE_UUID}'",
                "(`id` UInt64, `version` UInt64)",
                f"ENGINE = {engine}",
                "ORDER BY id",
            ]
        ),
        encoding="utf-8",
    )

    tables = load_offline_tables(
        metadata,
        {TABLE_UUID},
        {"shard": "01"},
        default_replica_path="/clickhouse/tables/{shard}/{uuid}",
    )

    assert tables[TABLE_UUID].zookeeper_path == TABLE_ZK_PATH


def test_keep_going_detaches_part_without_local_metadata(
    tmp_path: Path,
) -> None:
    store, table_path = make_store(tmp_path)
    source = make_part(table_path, "all_1_1_0")
    metadata = tmp_path / "metadata"

    report = quarantine_object_storage_parts(
        MagicMock(),
        str(store),
        str(metadata),
        None,
        dry_run=False,
        keep_going=True,
    )

    assert not source.exists()
    assert (table_path / "detached" / f"{DETACHED_PREFIX}all_1_1_0").is_dir()
    assert report.parts[0].table is None
    assert report.details()[0]["database"] is None


def test_missing_exact_keeper_part_is_nonfatal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, table_path = make_store(tmp_path)
    source = make_part(table_path, "all_1_1_0")
    metadata = make_metadata(tmp_path)
    zk = make_zookeeper(set())

    @contextmanager
    def fake_zk_client(_ctx: object) -> Generator[FakeZooKeeper, None, None]:
        yield zk

    monkeypatch.setattr(parts_quarantine, "zk_client", fake_zk_client)

    report = quarantine_object_storage_parts(
        MagicMock(), str(store), str(metadata), None, dry_run=False
    )

    assert not source.exists()
    assert report.moved == 1
    assert report.zookeeper_nodes == 0


def test_keep_going_skips_missing_replica_parts_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, table_path = make_store(tmp_path)
    source = make_part(table_path, "all_1_1_0")
    metadata = make_metadata(tmp_path)
    zk = make_zookeeper({"all_1_1_0"})
    missing_path = f"{TABLE_ZK_PATH}/replicas/r2/parts"
    del zk.children[f"{missing_path}/all_1_1_0"]
    del zk.children[missing_path]

    @contextmanager
    def fake_zk_client(_ctx: object) -> Generator[FakeZooKeeper, None, None]:
        yield zk

    monkeypatch.setattr(parts_quarantine, "zk_client", fake_zk_client)

    report = quarantine_object_storage_parts(
        MagicMock(),
        str(store),
        str(metadata),
        None,
        dry_run=False,
        keep_going=True,
    )

    assert not source.exists()
    assert report.zookeeper_nodes == 1
    assert zk.deleted == [f"{TABLE_ZK_PATH}/replicas/r1/parts/all_1_1_0"]


def test_stale_local_keeper_is_active_is_ignored() -> None:
    zk = make_zookeeper({"all_1_1_0"})
    zk.children[f"{TABLE_ZK_PATH}/replicas/r1/is_active"] = set()
    table = OfflineTable("test", "table", TABLE_UUID, TABLE_ZK_PATH, replica_name="r1")

    ensure_nonlocal_zookeeper_replicas_inactive(
        zk,
        {TABLE_UUID: table},
    )


def test_active_replica_blocks_operation_before_local_move(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, table_path = make_store(tmp_path)
    source = make_part(table_path, "all_1_1_0")
    metadata = make_metadata(tmp_path)
    zk = make_zookeeper({"all_1_1_0"})
    zk.children[f"{TABLE_ZK_PATH}/replicas/r2/is_active"] = set()

    @contextmanager
    def fake_zk_client(_ctx: object) -> Generator[FakeZooKeeper, None, None]:
        yield zk

    monkeypatch.setattr(parts_quarantine, "zk_client", fake_zk_client)

    with pytest.raises(RuntimeError, match="Keeper replica is active"):
        quarantine_object_storage_parts(
            MagicMock(), str(store), str(metadata), None, dry_run=False
        )

    assert source.is_dir()
    assert not zk.deleted


def test_detects_running_clickhouse_service(monkeypatch: pytest.MonkeyPatch) -> None:
    check_output = MagicMock(return_value="deactivating\n")
    monkeypatch.setattr(system_module, "_systemd_is_running", lambda: True)
    monkeypatch.setattr(system_module, "_running_clickhouse_server_pids", lambda: [])
    monkeypatch.setattr(system_module.subprocess, "check_output", check_output)

    with pytest.raises(RuntimeError, match="systemd service is not stopped"):
        ensure_clickhouse_server_stopped()

    assert check_output.call_args.args[0][0] == SYSTEMCTL_PATH


def test_detects_running_clickhouse_process_without_systemd(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(system_module, "_systemd_is_running", lambda: False)
    monkeypatch.setattr(
        system_module, "_running_clickhouse_server_pids", lambda: [123, 456]
    )

    with pytest.raises(RuntimeError, match=r"\[123, 456\]"):
        ensure_clickhouse_server_stopped()


def test_enabled_broken_detached_cleanup_blocks_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        parts_quarantine,
        "get_clickhouse_config",
        lambda _ctx: SimpleNamespace(
            merge_tree_settings={"merge_tree_enable_clear_old_broken_detached": "1"}
        ),
    )

    with pytest.raises(RuntimeError, match="can delete quarantined parts"):
        ensure_broken_detached_parts_are_not_cleaned(MagicMock())


def test_table_setting_for_broken_detached_cleanup_blocks_operation(
    tmp_path: Path,
) -> None:
    metadata = make_metadata(tmp_path)
    metadata_file = metadata / "test" / "table.sql"
    metadata_file.write_text(
        metadata_file.read_text(encoding="utf-8")
        + "\nSETTINGS merge_tree_enable_clear_old_broken_detached = 1\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="can delete quarantined parts"):
        load_offline_tables(
            metadata,
            {TABLE_UUID},
            {"shard": "01"},
            keep_going=True,
        )


def test_rename_preserves_part_ownership_and_mode(tmp_path: Path) -> None:
    store, table_path = make_store(tmp_path)
    source = make_part(table_path, "all_1_1_0")
    source.chmod(0o750)
    source_stat = source.stat()
    parts = discover_object_storage_parts(store)

    parts_quarantine.move_parts_to_detached(parts)

    target_stat = parts[0].detached_path.stat()
    assert target_stat.st_uid == source_stat.st_uid
    assert target_stat.st_gid == source_stat.st_gid
    assert target_stat.st_mode == source_stat.st_mode


def test_cli_builds_store_path_from_disk_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "disks" / "cold_s3").mkdir(parents=True)
    quarantine = MagicMock(return_value=QuarantineReport([], 0, 0, 0, dry_run=True))
    monkeypatch.setattr(
        data_store_group_module, "quarantine_object_storage_parts", quarantine
    )
    monkeypatch.setattr(data_store_group_module, "print_response", MagicMock())
    monkeypatch.setattr(
        data_store_group_module,
        "get_clickhouse_config",
        lambda _ctx: SimpleNamespace(data_path=str(tmp_path)),
    )

    result = CliRunner().invoke(
        data_store_group,
        [
            "detach-object-storage-parts",
            "--disk",
            "cold_s3",
            "--detached-prefix",
            "quarantine_",
            "--keep-going",
            "--no-mark-replicas-lost",
            "--dry-run",
        ],
        obj={"config": {"loguru": {"handlers": {}, "formatters": {}}}},
    )

    assert result.exit_code == 0, result.output
    quarantine.assert_called_once_with(
        ctx=ANY,
        root_path=str(tmp_path / "disks" / "cold_s3" / "store"),
        metadata_path=str(tmp_path / "metadata"),
        parts_file=None,
        dry_run=True,
        detached_prefix="quarantine_",
        keep_going=True,
        mark_replicas_lost=False,
    )


@pytest.mark.parametrize("disk_name", ["../object_storage", "/tmp/disk", "a/b", "."])
def test_cli_rejects_disk_name_that_is_not_one_component(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, disk_name: str
) -> None:
    (tmp_path / "disks" / "object_storage").mkdir(parents=True)
    quarantine = MagicMock()
    monkeypatch.setattr(
        data_store_group_module, "quarantine_object_storage_parts", quarantine
    )
    monkeypatch.setattr(
        data_store_group_module,
        "get_clickhouse_config",
        lambda _ctx: SimpleNamespace(data_path=str(tmp_path)),
    )

    result = CliRunner().invoke(
        data_store_group,
        ["detach-object-storage-parts", "--disk", disk_name, "--dry-run"],
        obj={"config": {"loguru": {"handlers": {}, "formatters": {}}}},
    )

    assert result.exit_code == 1
    assert "is not a directory under" in result.output
    quarantine.assert_not_called()
