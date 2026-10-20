from unittest.mock import MagicMock, call

import pytest
from botocore.exceptions import ClientError

from ch_tools.chadmin.internal.object_storage import (
    broken_partitions_recovery as recovery,
)
from ch_tools.common import logging


@pytest.fixture(autouse=True)
def configure_logging() -> None:
    logging.configure({"handlers": {}, "formatters": {}}, __name__)


def test_head_mode_uses_head_object() -> None:
    client = MagicMock()

    assert recovery.check_object_in_storage(
        client, "bucket", "object", expected_size=100, mode="head"
    )
    client.head_object.assert_called_once_with(Bucket="bucket", Key="object")
    client.get_object.assert_not_called()


def test_range_mode_requests_and_reads_the_first_byte() -> None:
    body = MagicMock()
    body.read.return_value = b"x"
    client = MagicMock()
    client.get_object.return_value = {"Body": body}

    assert recovery.check_object_in_storage(
        client, "bucket", "object", expected_size=100, mode="range"
    )
    client.get_object.assert_called_once_with(
        Bucket="bucket", Key="object", Range="bytes=0-0"
    )
    body.read.assert_called_once_with(1)
    body.close.assert_called_once_with()


@pytest.mark.parametrize(
    "content,expected",
    [
        (b"", True),
        (b"x", False),
    ],
)
def test_range_mode_gets_zero_size_objects_without_range(
    content: bytes, expected: bool
) -> None:
    body = MagicMock()
    body.read.return_value = content
    client = MagicMock()
    client.get_object.return_value = {"Body": body}

    assert (
        recovery.check_object_in_storage(
            client, "bucket", "empty", expected_size=0, mode="range"
        )
        is expected
    )
    client.get_object.assert_called_once_with(Bucket="bucket", Key="empty")
    body.read.assert_called_once_with(1)
    body.close.assert_called_once_with()


@pytest.mark.parametrize("expected_size,expected", [(3, True), (4, False)])
def test_full_mode_streams_to_eof_and_checks_the_size(
    expected_size: int, expected: bool
) -> None:
    body = MagicMock()
    body.read.side_effect = [b"ab", b"c", b""]
    client = MagicMock()
    client.get_object.return_value = {"Body": body}

    assert (
        recovery.check_object_in_storage(
            client,
            "bucket",
            "object",
            expected_size=expected_size,
            mode="full",
        )
        is expected
    )
    client.get_object.assert_called_once_with(Bucket="bucket", Key="object")
    assert body.read.call_args_list == [
        call(recovery.READ_CHUNK_SIZE),
        call(recovery.READ_CHUNK_SIZE),
        call(recovery.READ_CHUNK_SIZE),
    ]
    body.close.assert_called_once_with()


def test_get_or_stream_error_fails_the_object_check() -> None:
    client = MagicMock()
    client.get_object.side_effect = ClientError(
        {"Error": {"Code": "InternalError", "Message": "unreadable"}},
        "GetObject",
    )

    assert not recovery.check_object_in_storage(
        client, "bucket", "object", expected_size=1, mode="range"
    )

    body = MagicMock()
    body.read.side_effect = OSError("stream failed")
    client.get_object.side_effect = None
    client.get_object.return_value = {"Body": body}
    assert not recovery.check_object_in_storage(
        client, "bucket", "object", expected_size=1, mode="full"
    )
    body.close.assert_called_once_with()
