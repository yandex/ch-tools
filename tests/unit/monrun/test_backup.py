from typing import Dict, Sequence

from pytest import mark

from ch_tools.monrun_checks.ch_backup import _is_backup_failed_by_userfault_error


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
def test_is_backup_failed_by_userfault_error(
    backups: Sequence[Dict], result: bool
) -> None:
    assert _is_backup_failed_by_userfault_error(backups) == result
