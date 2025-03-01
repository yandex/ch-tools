from datetime import datetime, timedelta, timezone
from typing import Dict, List, Sequence

from click import Command, Context
from pytest import mark

from ch_tools.common.config import DEFAULT_CONFIG
from ch_tools.common.result import CRIT, OK, WARNING
from ch_tools.monrun_checks.ch_backup import (
    _check_backup_age,
    _is_backup_failed_by_user_fault_error,
)


@mark.parametrize(
    ["backups", "result"],
    [
        (({"state": "created"},), False),
        (
            (
                {"state": "failed"},
                {"state": "created"},
            ),
            False,
        ),
        (
            (
                {"state": "failed"},
                {"state": "failed"},
                {"state": "created"},
            ),
            False,
        ),
        (
            (
                {"state": "failed", "exception": None},
                {"state": "created"},
            ),
            False,
        ),
        (
            (
                {
                    "state": "failed",
                    "exception": "ClickhouseError: Code: 243. DB::Exception: Cannot reserve 1.00 MiB, not enough space. (NOT_ENOUGH_SPACE) (version 23.8.14.6 (official build))",
                },
                {"state": "failed"},
                {"state": "failed"},
                {"state": "created"},
            ),
            True,
        ),
        (
            (
                {"state": "failed", "exception": "God's will"},
                {"state": "failed"},
                {"state": "failed"},
                {"state": "created"},
            ),
            False,
        ),
        (
            (
                {"state": "failed", "exception": "Disk quota exceeded"},
                {"state": "failed", "exception": "Disk quota exceeded"},
                {"state": "failed", "exception": "Disk quota exceeded"},
                {"state": "created"},
            ),
            True,
        ),
    ],
)
def test_is_backup_failed_by_user_fault_error(
    backups: Sequence[Dict], result: bool
) -> None:
    ctx = Context(Command("backup"), obj=dict(config=DEFAULT_CONFIG))
    assert _is_backup_failed_by_user_fault_error(ctx, backups) == result


@mark.parametrize(
    ["backups", "expected_code", "expected_message"],
    [
        (
            [],
            OK,
            "OK",
        ),
        (
            [
                {
                    "state": "created",
                    "start_time": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S %z"
                    ),
                }
            ],
            OK,
            "OK",
        ),
        (
            [
                {
                    "state": "created",
                    "start_time": (
                        datetime.now(timezone.utc) - timedelta(days=1)
                    ).strftime("%Y-%m-%d %H:%M:%S %z"),
                }
            ],
            OK,
            "OK",
        ),
        (
            [
                {
                    "state": "created",
                    "start_time": (
                        datetime.now(timezone.utc) - timedelta(days=3)
                    ).strftime("%Y-%m-%d %H:%M:%S %z"),
                }
            ],
            WARNING,
            "Last backup was created 3 days ago",
        ),
        (
            [
                {
                    "state": "created",
                    "start_time": (
                        datetime.now(timezone.utc) - timedelta(days=5)
                    ).strftime("%Y-%m-%d %H:%M:%S %z"),
                }
            ],
            CRIT,
            "Last backup was created 5 days ago",
        ),
        (
            [
                {
                    "state": "creating",
                    "start_time": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S %z"
                    ),
                },
                {
                    "state": "failed",
                    "start_time": (
                        datetime.now(timezone.utc) - timedelta(days=3)
                    ).strftime("%Y-%m-%d %H:%M:%S %z"),
                },
            ],
            OK,
            "OK",
        ),
        (
            [
                {
                    "state": "creating",
                    "start_time": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S %z"
                    ),
                },
                {
                    "state": "failed",
                    "start_time": (
                        datetime.now(timezone.utc) - timedelta(days=3)
                    ).strftime("%Y-%m-%d %H:%M:%S %z"),
                },
                {
                    "state": "created",
                    "start_time": (
                        datetime.now(timezone.utc) - timedelta(days=5)
                    ).strftime("%Y-%m-%d %H:%M:%S %z"),
                },
            ],
            CRIT,
            "Last backup was created 5 days ago",
        ),
    ],
)
def test_check_backup_age(
    backups: List[Dict],
    expected_code: int,
    expected_message: str,
) -> None:
    check_result = _check_backup_age(
        backups,
        timedelta(days=3),
        timedelta(days=5),
    )

    assert check_result.code == expected_code
    assert check_result.message == expected_message
