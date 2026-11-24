from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ch_tools.chadmin.internal import table
from ch_tools.chadmin.internal.table_metadata_manager import TableMetadataManager
from ch_tools.chadmin.internal.table_metadata_parser import (
    MergeTreeFamilyEngines,
    MetadataParseError,
    TableMetadata,
)


@pytest.mark.parametrize(
    "disk_type,expected_path",
    [
        (
            table.DISK_LOCAL_KEY,
            "/var/lib/clickhouse/store/123/12345678-1234-1234-1234-123456789012",
        ),
        (
            table.DISK_OBJECT_STORAGE_KEY,
            "/var/lib/clickhouse/disks/object_storage/store/123/12345678-1234-1234-1234-123456789012",
        ),
    ],
)
def test_disk_remover_checks_path_under_disk_root(
    monkeypatch: pytest.MonkeyPatch,
    disk_type: str,
    expected_path: str,
) -> None:
    exists = MagicMock(return_value=True)
    monkeypatch.setattr(table.os.path, "exists", exists)

    assert table._is_should_use_ch_disk_remover(
        "store/123/12345678-1234-1234-1234-123456789012",
        disk_type,
    )
    exists.assert_called_once_with(expected_path)


def test_remove_table_data_skips_disk_without_table_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    should_remove = MagicMock(return_value=False)
    make_config = MagicMock()
    remove = MagicMock()
    monkeypatch.setattr(table, "_is_should_use_ch_disk_remover", should_remove)
    monkeypatch.setattr(table, "make_ch_disks_config", make_config)
    monkeypatch.setattr(table, "remove_from_ch_disk", remove)
    monkeypatch.setattr(table, "logging", MagicMock())

    table._remove_table_data_from_disk(
        "12345678-1234-1234-1234-123456789012",
        "object_storage",
        table.DISK_OBJECT_STORAGE_KEY,
        "25.3.12.8",
    )

    should_remove.assert_called_once_with(
        "store/123/12345678-1234-1234-1234-123456789012",
        table.DISK_OBJECT_STORAGE_KEY,
    )
    make_config.assert_not_called()
    remove.assert_not_called()


def test_remove_table_data_uses_clickhouse_disks_for_existing_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        table,
        "_is_should_use_ch_disk_remover",
        MagicMock(return_value=True),
    )
    make_config = MagicMock(return_value="/tmp/disks.xml")
    remove = MagicMock(return_value=(0, b""))
    monkeypatch.setattr(table, "make_ch_disks_config", make_config)
    monkeypatch.setattr(table, "remove_from_ch_disk", remove)
    monkeypatch.setattr(table, "logging", MagicMock())

    table._remove_table_data_from_disk(
        "12345678-1234-1234-1234-123456789012",
        "default",
        table.DISK_LOCAL_KEY,
        "25.3.12.8",
    )

    make_config.assert_called_once_with("default")
    remove.assert_called_once_with(
        disk="default",
        path="store/123/12345678-1234-1234-1234-123456789012",
        disk_config_path="/tmp/disks.xml",
        ch_version="25.3.12.8",
    )


def test_delete_detached_table_resolves_defaults_before_removing_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata = TableMetadata(
        "12345678-1234-1234-1234-123456789012",
        MergeTreeFamilyEngines.REPLICATED_MERGE_TREE,
    )
    remove_data = MagicMock()
    monkeypatch.setattr(table, "check_table_dettached", MagicMock())
    monkeypatch.setattr(table.os.path, "exists", MagicMock(return_value=True))
    monkeypatch.setattr(
        table.TableMetadataParser, "parse", MagicMock(return_value=metadata)
    )
    monkeypatch.setattr(
        table,
        "get_clickhouse_config",
        lambda _ctx: SimpleNamespace(
            default_replica_path=None,
            default_replica_name=None,
        ),
    )
    monkeypatch.setattr(table, "_get_disks_data", remove_data)

    with pytest.raises(MetadataParseError, match="default_replica_path"):
        table.delete_detached_table(MagicMock(), "database", "table")

    remove_data.assert_not_called()


def test_uuid_validation_uses_default_replica_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = MagicMock()
    metadata = TableMetadata(
        "12345678-1234-1234-1234-123456789012",
        MergeTreeFamilyEngines.REPLICATED_MERGE_TREE,
    )
    shared_id = MagicMock(return_value=metadata.table_uuid)
    monkeypatch.setattr(
        "ch_tools.chadmin.internal.table_metadata_manager.TableMetadataParser.parse",
        MagicMock(return_value=metadata),
    )
    monkeypatch.setattr(
        "ch_tools.chadmin.internal.table_metadata_manager.get_clickhouse_config",
        lambda _ctx: SimpleNamespace(default_replica_path="/tables/default"),
    )
    monkeypatch.setattr(
        "ch_tools.chadmin.internal.table_metadata_manager.get_table_shared_id",
        shared_id,
    )

    TableMetadataManager._verify_possible_change_uuid(
        ctx, "/metadata/table.sql", metadata.table_uuid
    )

    shared_id.assert_called_once_with(ctx, "/tables/default")
