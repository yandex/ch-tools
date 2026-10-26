from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Generator
from unittest.mock import ANY, MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from ch_tools.chadmin.internal.object_storage.checked_partition_attach import (
    S3ObjectChecker,
    attach_checked_partitions,
)
from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    S3ObjectLocalInfo,
)
from ch_tools.common import logging
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration


@pytest.fixture(autouse=True)
def configure_logging() -> None:
    logging.configure({"handlers": {}, "formatters": {}}, __name__)


def client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code}}, "GetObject")


def object_info(size: int = 3, key: str = "prefix/object") -> S3ObjectLocalInfo:
    return S3ObjectLocalInfo(key=key, size=size, key_is_full=True)


def s3_disk_config() -> S3DiskConfiguration:
    return S3DiskConfiguration(
        name="object_storage",
        endpoint_url="https://s3.example.test",
        access_key_id="access-key",
        secret_access_key="secret-key",
        bucket_name="bucket",
        prefix="prefix",
    )


def test_head_checks_reported_size() -> None:
    client = MagicMock()
    client.head_object.return_value = {"ContentLength": 3}
    checker = S3ObjectChecker(client, "bucket", "unused", "head")

    assert checker.check(object_info()) is None
    result = checker.check(object_info(size=4))
    assert result is not None
    assert "reported 3" in result
    client.head_object.assert_called_once_with(Bucket="bucket", Key="prefix/object")


def test_get_reads_the_whole_object_and_checks_actual_size() -> None:
    client = MagicMock()
    client.get_object.return_value = {
        "ContentLength": 3,
        "Body": BytesIO(b"ab"),
    }
    checker = S3ObjectChecker(client, "bucket", "unused", "get")

    result = checker.check(object_info())
    assert result is not None
    assert "expected 3, read 2" in result


@pytest.mark.parametrize("method", ["head", "get"])
@pytest.mark.parametrize("code", ["404", "NoSuchKey", "NotFound"])
def test_missing_object_is_a_failed_check(method: str, code: str) -> None:
    client = MagicMock()
    getattr(client, f"{method}_object").side_effect = client_error(code)
    checker = S3ObjectChecker(client, "bucket", "unused", method)

    assert checker.check(object_info()) == "S3 object is missing: prefix/object"


def test_s3_server_error_is_not_hidden() -> None:
    client = MagicMock()
    client.head_object.side_effect = client_error("InternalError")
    checker = S3ObjectChecker(client, "bucket", "unused", "head")

    with pytest.raises(ClientError):
        checker.check(object_info())


def test_unknown_s3_check_method_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unsupported S3 check method"):
        S3ObjectChecker(MagicMock(), "bucket", "unused", "invalid")

    with pytest.raises(ValueError, match="Unsupported S3 check method"):
        attach_checked_partitions(
            MagicMock(),
            [],
            reason="",
            check_method="invalid",
            dry_run=False,
            keep_going=True,
        )


def test_s3_server_error_happens_before_any_prefix_is_removed(
    tmp_path: Path,
) -> None:
    detached = tmp_path / "detached"
    first_path = detached / "broken-on-start_all_1_1_0"
    second_path = detached / "broken-on-start_all_2_2_0"
    write_metadata(first_path, "prefix/first")
    write_metadata(second_path, "prefix/second")
    parts = [
        part_row(first_path, "all_1_1_0"),
        part_row(second_path, "all_2_2_0"),
    ]

    client = MagicMock()
    client.head_object.side_effect = [
        {"ContentLength": 3},
        client_error("InternalError"),
    ]
    checker = S3ObjectChecker(client, "bucket", "unused", "head")

    with (
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.list_detached_parts",
            return_value=parts,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach._make_s3_checker",
            return_value=checker,
        ),
    ):
        with pytest.raises(ClientError):
            attach_checked_partitions(
                MagicMock(),
                [{"database": "db", "table": "table", "partition_id": "all"}],
                reason="broken-on-start",
                check_method="head",
                dry_run=False,
                keep_going=False,
            )

    assert first_path.is_dir()
    assert second_path.is_dir()
    assert not (detached / "all_1_1_0").exists()


def test_clickhouse_without_detached_part_paths_is_rejected() -> None:
    part = part_row(Path("unused"), "all_1_1_0")
    part.pop("path")

    with patch(
        "ch_tools.chadmin.internal.object_storage.checked_partition_attach.list_detached_parts",
        return_value=[part],
    ):
        with pytest.raises(RuntimeError, match="available since ClickHouse 23.1"):
            attach_checked_partitions(
                MagicMock(),
                [{"database": "db", "table": "table", "partition_id": "all"}],
                reason="broken-on-start",
                check_method="head",
                dry_run=False,
                keep_going=False,
            )


def write_metadata(
    part_path: Path,
    key: str,
    size: int = 3,
    filename: str = "data.bin",
) -> None:
    part_path.mkdir(parents=True, exist_ok=True)
    (part_path / filename).write_text(
        f"5\n1\t{size}\n{size}\t{key}\n0\n0\n",
        encoding="latin-1",
    )


def part_row(path: Path, name: str, reason: str = "broken-on-start") -> Dict[str, Any]:
    return {
        "database": "db",
        "table": "table",
        "partition_id": "all",
        "name": name,
        "disk_name": "object_storage",
        "path": str(path),
        "reason": reason,
    }


def test_reason_attach_moves_only_parts_that_pass_the_check(tmp_path: Path) -> None:
    detached = tmp_path / "detached"
    good_path = detached / "broken-on-start_all_1_1_0"
    broken_path = detached / "broken-on-start_all_2_2_0"
    unprefixed_path = detached / "all_3_3_0"
    write_metadata(good_path, "prefix/good")
    write_metadata(broken_path, "prefix/missing")
    write_metadata(unprefixed_path, "prefix/unprefixed")
    parts = [
        part_row(good_path, "all_1_1_0"),
        part_row(broken_path, "all_2_2_0"),
        part_row(unprefixed_path, "all_3_3_0", reason=""),
    ]

    client = MagicMock()

    def head_object(*, Bucket: str, Key: str) -> Dict[str, int]:
        assert Bucket == "bucket"
        if Key == "prefix/missing":
            raise client_error("NoSuchKey")
        return {"ContentLength": 3}

    client.head_object.side_effect = head_object
    checker = S3ObjectChecker(client, "bucket", "unused", "head")
    ctx = MagicMock()

    with (
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.list_detached_parts",
            return_value=parts,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach._make_s3_checker",
            return_value=checker,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.attach_partition"
        ) as attach_partition,
    ):
        report = attach_checked_partitions(
            ctx,
            [{"database": "db", "table": "table", "partition_id": "all"}],
            reason="broken-on-start",
            check_method="head",
            dry_run=False,
            keep_going=False,
        )

    assert not good_path.exists()
    assert (detached / "all_1_1_0").is_dir()
    assert broken_path.is_dir()
    assert unprefixed_path.is_dir()
    assert [item["check"] for item in report] == [
        "passed",
        "failed",
        "passed",
    ]
    assert [item["action"] for item in report] == [
        "attached",
        "left-prefixed",
        "attached",
    ]
    attach_partition.assert_called_once_with(
        ctx,
        database="db",
        table="table",
        partition_id="all",
        dry_run=False,
    )


def test_failed_unprefixed_part_blocks_reason_partition_attach(
    tmp_path: Path,
) -> None:
    detached = tmp_path / "detached"
    prefixed_path = detached / "broken-on-start_all_1_1_0"
    unprefixed_path = detached / "all_1_1_0"
    write_metadata(prefixed_path, "prefix/good")
    write_metadata(unprefixed_path, "prefix/missing")
    parts = [
        part_row(prefixed_path, "all_1_1_0"),
        part_row(unprefixed_path, "all_1_1_0", reason=""),
    ]

    client = MagicMock()

    def head_object(*, Bucket: str, Key: str) -> Dict[str, int]:
        assert Bucket == "bucket"
        if Key == "prefix/missing":
            raise client_error("NoSuchKey")
        return {"ContentLength": 3}

    client.head_object.side_effect = head_object
    checker = S3ObjectChecker(client, "bucket", "unused", "head")

    with (
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.list_detached_parts",
            return_value=parts,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach._make_s3_checker",
            return_value=checker,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.attach_partition"
        ) as attach_partition,
    ):
        report = attach_checked_partitions(
            MagicMock(),
            [{"database": "db", "table": "table", "partition_id": "all"}],
            reason="broken-on-start",
            check_method="head",
            dry_run=False,
            keep_going=False,
        )

    assert prefixed_path.is_dir()
    assert unprefixed_path.is_dir()
    assert [item["check"] for item in report] == ["passed", "failed"]
    assert [item["action"] for item in report] == [
        "left-prefixed",
        "skipped",
    ]
    assert "unprefixed part" in report[0]["error"]
    attach_partition.assert_not_called()


def test_failed_unprefixed_part_is_quarantined_before_partition_attach(
    tmp_path: Path,
) -> None:
    detached = tmp_path / "detached"
    prefixed_path = detached / "broken-on-start_all_1_1_0"
    unprefixed_path = detached / "all_1_1_0"
    write_metadata(prefixed_path, "prefix/good")
    write_metadata(unprefixed_path, "prefix/missing")
    parts = [
        part_row(prefixed_path, "all_1_1_0"),
        part_row(unprefixed_path, "all_1_1_0", reason=""),
    ]

    client = MagicMock()

    def head_object(*, Bucket: str, Key: str) -> Dict[str, int]:
        assert Bucket == "bucket"
        if Key == "prefix/missing":
            raise client_error("NoSuchKey")
        return {"ContentLength": 3}

    client.head_object.side_effect = head_object
    checker = S3ObjectChecker(client, "bucket", "unused", "head")

    with (
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.list_detached_parts",
            return_value=parts,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach._make_s3_checker",
            return_value=checker,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.attach_partition"
        ) as attach_partition,
    ):
        report = attach_checked_partitions(
            MagicMock(),
            [{"database": "db", "table": "table", "partition_id": "all"}],
            reason="broken-on-start",
            check_method="head",
            dry_run=False,
            keep_going=False,
            quarantine_failed_unprefixed="broken",
        )

    assert not prefixed_path.exists()
    assert (detached / "all_1_1_0").is_dir()
    assert "prefix/good" in (detached / "all_1_1_0" / "data.bin").read_text(
        encoding="latin-1"
    )
    assert (detached / "broken_all_1_1_0").is_dir()
    assert [item["action"] for item in report] == ["attached", "quarantined"]
    assert report[1]["destination_part"] == "broken_all_1_1_0"
    attach_partition.assert_called_once_with(
        ANY,
        database="db",
        table="table",
        partition_id="all",
        dry_run=False,
    )


def test_failed_unprefixed_part_is_not_quarantined_without_ready_parts(
    tmp_path: Path,
) -> None:
    detached = tmp_path / "detached"
    prefixed_path = detached / "broken-on-start_all_1_1_0"
    unprefixed_path = detached / "all_2_2_0"
    write_metadata(prefixed_path, "prefix/missing-prefixed")
    write_metadata(unprefixed_path, "prefix/missing-unprefixed")
    parts = [
        part_row(prefixed_path, "all_1_1_0"),
        part_row(unprefixed_path, "all_2_2_0", reason=""),
    ]

    client = MagicMock()
    client.head_object.side_effect = client_error("NoSuchKey")
    checker = S3ObjectChecker(client, "bucket", "unused", "head")

    with (
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.list_detached_parts",
            return_value=parts,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach._make_s3_checker",
            return_value=checker,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.attach_partition"
        ) as attach_partition,
    ):
        report = attach_checked_partitions(
            MagicMock(),
            [{"database": "db", "table": "table", "partition_id": "all"}],
            reason="broken-on-start",
            check_method="head",
            dry_run=False,
            keep_going=False,
            quarantine_failed_unprefixed="broken",
        )

    assert prefixed_path.is_dir()
    assert unprefixed_path.is_dir()
    assert not (detached / "broken_all_2_2_0").exists()
    assert [item["action"] for item in report] == ["left-prefixed", "skipped"]
    assert [item["quarantine_reason"] for item in report] == ["", ""]
    attach_partition.assert_not_called()


def test_check_without_reason_skips_whole_partition_on_failure(
    tmp_path: Path,
) -> None:
    detached = tmp_path / "detached"
    good_path = detached / "all_1_1_0"
    broken_path = detached / "all_2_2_0"
    write_metadata(good_path, "prefix/good")
    write_metadata(broken_path, "prefix/missing")
    parts = [
        part_row(good_path, "all_1_1_0", reason=""),
        part_row(broken_path, "all_2_2_0", reason=""),
    ]

    client = MagicMock()
    client.head_object.side_effect = [
        {"ContentLength": 3},
        client_error("NoSuchKey"),
    ]
    checker = S3ObjectChecker(client, "bucket", "unused", "head")

    with (
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.list_detached_parts",
            return_value=parts,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach._make_s3_checker",
            return_value=checker,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.attach_partition"
        ) as attach_partition,
    ):
        report = attach_checked_partitions(
            MagicMock(),
            [{"database": "db", "table": "table", "partition_id": "all"}],
            reason="",
            check_method="head",
            dry_run=False,
            keep_going=False,
        )

    assert [item["action"] for item in report] == ["skipped", "skipped"]
    attach_partition.assert_not_called()


def test_attach_failure_restores_reason_prefix(tmp_path: Path) -> None:
    detached = tmp_path / "detached"
    source = detached / "broken-on-start_all_1_1_0"
    write_metadata(source, "prefix/good")
    parts = [part_row(source, "broken-on-start_all_1_1_0")]

    with (
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.list_detached_parts",
            return_value=parts,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.attach_partition",
            side_effect=RuntimeError("attach failed"),
        ),
    ):
        report = attach_checked_partitions(
            MagicMock(),
            [{"database": "db", "table": "table", "partition_id": "all"}],
            reason="broken-on-start",
            check_method=None,
            dry_run=False,
            keep_going=True,
        )

    assert source.is_dir()
    assert not (detached / "all_1_1_0").exists()
    assert report[0]["action"] == "attach-failed"


def test_old_zero_copy_locks_are_deleted_after_successful_attach(
    tmp_path: Path,
) -> None:
    detached = tmp_path / "detached"
    source = detached / "broken-on-start_all_1_1_0"
    unprefixed = detached / "all_2_2_0"
    write_metadata(source, "prefix/good")
    write_metadata(source, "prefix/good", filename="checksums.txt")
    write_metadata(unprefixed, "prefix/unprefixed")
    write_metadata(unprefixed, "prefix/unprefixed", filename="checksums.txt")
    parts = [
        # system.detached_parts.name is the directory name, not the parsed
        # logical part name.
        part_row(source, "broken-on-start_all_1_1_0"),
        part_row(unprefixed, "all_2_2_0", reason=""),
    ]
    zk = MagicMock()
    events = []

    @contextmanager
    def fake_zk_client(_ctx: Any) -> Generator[Any, None, None]:
        yield zk

    def record_attach(*args: Any, **kwargs: Any) -> None:
        events.append("attach")

    def record_delete(*args: Any, **kwargs: Any) -> None:
        events.append("delete-locks")

    storage_configuration = MagicMock()
    storage_configuration.get_disk_config.return_value = {"type": "s3"}
    clickhouse_config = MagicMock(storage_configuration=storage_configuration)

    with (
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.list_detached_parts",
            return_value=parts,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.get_clickhouse_config",
            return_value=clickhouse_config,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.get_table",
            return_value={"uuid": "local-uuid"},
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.get_table_replica",
            return_value={"zookeeper_path": "/tables/table"},
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.get_table_shared_id",
            return_value="shared-uuid",
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach._get_zero_copy_zookeeper_path_for_disk_type",
            return_value="/clickhouse/zero_copy/zero_copy_s3",
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.S3DiskConfiguration.from_config",
            return_value=s3_disk_config(),
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.zk_client",
            side_effect=fake_zk_client,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.attach_partition",
            side_effect=record_attach,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.delete_recursive",
            side_effect=record_delete,
        ) as delete_recursive,
    ):
        report = attach_checked_partitions(
            MagicMock(),
            [{"database": "db", "table": "table", "partition_id": "all"}],
            reason="broken-on-start",
            check_method=None,
            dry_run=False,
            keep_going=False,
            cleanup_old_zero_copy_locks=True,
        )

    assert events == ["attach", "delete-locks"]
    delete_recursive.assert_called_once_with(
        zk,
        [
            "/clickhouse/zero_copy/zero_copy_s3/" "shared-uuid/all_1_1_0/prefix_good",
            "/clickhouse/zero_copy/zero_copy_s3/"
            "shared-uuid/all_2_2_0/prefix_unprefixed",
        ],
        dry_run=False,
    )
    assert report[0]["old_zero_copy_locks"] == "deleted"
    assert report[1]["old_zero_copy_locks"] == "deleted"


def test_old_zero_copy_locks_are_not_deleted_when_attach_fails(
    tmp_path: Path,
) -> None:
    detached = tmp_path / "detached"
    source = detached / "broken-on-start_all_1_1_0"
    write_metadata(source, "prefix/good")
    write_metadata(source, "prefix/good", filename="checksums.txt")
    parts = [part_row(source, "broken-on-start_all_1_1_0")]

    storage_configuration = MagicMock()
    storage_configuration.get_disk_config.return_value = {"type": "s3"}
    clickhouse_config = MagicMock(storage_configuration=storage_configuration)

    with (
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.list_detached_parts",
            return_value=parts,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.get_clickhouse_config",
            return_value=clickhouse_config,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.get_table",
            return_value={"uuid": "local-uuid"},
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.get_table_replica",
            return_value={"zookeeper_path": "/tables/table"},
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.get_table_shared_id",
            return_value="shared-uuid",
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach._get_zero_copy_zookeeper_path_for_disk_type",
            return_value="/clickhouse/zero_copy/zero_copy_s3",
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.S3DiskConfiguration.from_config",
            return_value=s3_disk_config(),
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.zk_client"
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.attach_partition",
            side_effect=RuntimeError("attach failed"),
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.delete_recursive"
        ) as delete_recursive,
    ):
        report = attach_checked_partitions(
            MagicMock(),
            [{"database": "db", "table": "table", "partition_id": "all"}],
            reason="broken-on-start",
            check_method=None,
            dry_run=False,
            keep_going=True,
            cleanup_old_zero_copy_locks=True,
        )

    delete_recursive.assert_not_called()
    assert report[0]["action"] == "attach-failed"
    assert report[0]["old_zero_copy_locks"] == "not-deleted"


def test_zero_copy_cleanup_is_preflighted_before_part_is_moved(
    tmp_path: Path,
) -> None:
    detached = tmp_path / "detached"
    source = detached / "broken-on-start_all_1_1_0"
    write_metadata(source, "prefix/good")
    parts = [part_row(source, "broken-on-start_all_1_1_0")]

    storage_configuration = MagicMock()
    storage_configuration.get_disk_config.return_value = {"type": "s3"}
    clickhouse_config = MagicMock(storage_configuration=storage_configuration)

    with (
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.list_detached_parts",
            return_value=parts,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.get_clickhouse_config",
            return_value=clickhouse_config,
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.get_table",
            return_value={"uuid": "local-uuid"},
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.get_table_replica",
            return_value={"zookeeper_path": "/tables/table"},
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.get_table_shared_id",
            return_value="",
        ),
        patch(
            "ch_tools.chadmin.internal.object_storage.checked_partition_attach.attach_partition"
        ) as attach_partition,
    ):
        with pytest.raises(RuntimeError, match="no table_shared_id"):
            attach_checked_partitions(
                MagicMock(),
                [{"database": "db", "table": "table", "partition_id": "all"}],
                reason="broken-on-start",
                check_method=None,
                dry_run=False,
                keep_going=False,
                cleanup_old_zero_copy_locks=True,
            )

    assert source.is_dir()
    assert not (detached / "all_1_1_0").exists()
    attach_partition.assert_not_called()


def test_old_zero_copy_lock_cleanup_requires_reason() -> None:
    with pytest.raises(ValueError, match="--reason"):
        attach_checked_partitions(
            MagicMock(),
            [{"database": "db", "table": "table", "partition_id": "all"}],
            reason="",
            check_method=None,
            dry_run=False,
            keep_going=False,
            cleanup_old_zero_copy_locks=True,
        )
