from typing import Dict, Sequence

from hamcrest import assert_that, equal_to
from pytest import mark

from ch_tools.common.result import CRIT, OK, WARNING
from ch_tools.monrun_checks.ch_backup import _check_last_backup_not_failed


@mark.parametrize(
    ["backups", "status_expected"],
    [
        (({"state": "created"},), OK),
        (
            (
                {"state": "failed", "fail_reason": None},
                {"state": "created"},
            ),
            WARNING,
        ),
        (
            (
                {"state": "failed"},
                {"state": "created"},
            ),
            WARNING,
        ),
        (
            (
                {"state": "failed"},
                {"state": "failed"},
                {"state": "created"},
            ),
            WARNING,
        ),
        (
            (
                {"state": "failed"},
                {"state": "failed"},
                {"state": "failed"},
                {"state": "created"},
            ),
            CRIT,
        ),
        (
            (
                {"state": "failed", "fail_reason": "Disk quota exceeded"},
                {"state": "failed"},
                {"state": "failed"},
                {"state": "created"},
            ),
            WARNING,
        ),
        (
            (
                {"state": "failed", "fail_reason": "God's will"},
                {"state": "failed"},
                {"state": "failed"},
                {"state": "created"},
            ),
            CRIT,
        ),
        (
            (
                {"state": "failed", "fail_reason": None},
                {"state": "failed"},
                {"state": "failed"},
                {"state": "created"},
            ),
            CRIT,
        ),
        (
            (
                {"state": "failed", "fail_reason": "Disk quota exceeded"},
                {"state": "failed", "fail_reason": "Disk quota exceeded"},
                {"state": "failed", "fail_reason": "Disk quota exceeded"},
                {"state": "created"},
            ),
            WARNING,
        ),
    ],
)
def test_last_backup_not_failed(
    backups: Sequence[Dict], status_expected: Sequence[int]
) -> None:
    assert_that(
        _check_last_backup_not_failed(list(backups), 3).code, equal_to(status_expected)
    )
