import pytest

from ch_tools.common.utils import version_ge, version_lt


@pytest.mark.parametrize(
    "version1,version2,expected",
    [
        ("22.8.21.38", "22.8.21.38", True),
        ("24.10.4.191", "22.8.21.38", True),
        ("24.10.4.191", "22.8", True),
        ("22.8.21.38", "24.10.4.191", False),
        ("24.10.4.191.dev", "22.8.21.38", True),
        ("24.10.4.191-dev.1", "22.8.21.38", True),
        ("22.8.21.38", "24.10.4.191-dev.1", False),
        ("24.10.4.191-dev.1", "24.10.4.191", True),
    ],
)
def test_version_ge(version1: str, version2: str, expected: bool) -> None:
    assert version_ge(version1, version2) == expected


@pytest.mark.parametrize(
    "version1,version2,expected",
    [
        ("22.8.21.38", "22.8.21.38", False),
        ("24.10.4.191", "22.8.21.38", False),
        ("22.8.21.38", "24.10.4.191", True),
        ("22.8.21.38", "24.10", True),
        ("22.8.21.38", "24.10.4.191.dev", True),
        ("22.8.21.38", "24.10.4.191-dev.1", True),
        ("24.10.4.191-dev.1", "24.10.4.191", False),
    ],
)
def test_version_lt(version1: str, version2: str, expected: bool) -> None:
    assert version_lt(version1, version2) == expected
