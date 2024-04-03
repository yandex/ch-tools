from typing import Dict, Sequence

from hamcrest import assert_that, equal_to
from pytest import mark

from ch_tools.monrun_checks.ch_backup import _count_failed_backups


@mark.parametrize(
    ["backups", "userfault_expected"],
    [
        (({"state": "created"},), 0),
        (
            (
                {"state": "failed", "exception": None},
                {"state": "created"},
            ),
            0,
        ),
        (
            (
                {"state": "failed"},
                {"state": "created"},
            ),
            0,
        ),
        (
            (
                {"state": "failed"},
                {"state": "failed"},
                {"state": "created"},
            ),
            0,
        ),
        (
            (
                {"state": "failed", "exception": "Disk quota exceeded"},
                {"state": "failed"},
                {"state": "failed"},
                {"state": "created"},
            ),
            1,
        ),
        (
            (
                {"state": "failed", "exception": "God's will"},
                {"state": "failed"},
                {"state": "failed"},
                {"state": "created"},
            ),
            0,
        ),
        (
            (
                {"state": "failed", "exception": None},
                {"state": "failed"},
                {"state": "failed"},
                {"state": "created"},
            ),
            0,
        ),
        (
            (
                {"state": "failed", "exception": "Disk quota exceeded"},
                {"state": "failed", "exception": "Disk quota exceeded"},
                {"state": "failed", "exception": "Disk quota exceeded"},
                {"state": "created"},
            ),
            3,
        ),
    ],
)
def test_last_backup_not_failed(
    backups: Sequence[Dict], userfault_expected: Sequence[int]
) -> None:
    _, userfault = _count_failed_backups(list(backups))
    assert_that(userfault, equal_to(userfault_expected))
