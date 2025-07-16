import pytest

from ch_tools.chadmin.internal.system import match_str_ch_version, validate_version


@pytest.mark.parametrize(
    "version",
    [
        "22.8.21.38",
        "24.8.15.1",
        "24.10.4.191",
        "24.10.4.191-lts",
        "24.10.4.191-lts-645f29c",
    ],
)
def test_validate_version(version: str) -> None:
    validate_version(version)


@pytest.mark.parametrize(
    "version,min_version",
    [
        ("22.8.21.38", "22.8.21.38"),
        ("24.10.4.191", "22.8.21.38"),
        ("24.10.4.191-lts", "22.8.21.38"),
        ("24.10.4.191", "22.8.21.38-lts"),
        ("24.10.4.191-lts", "22.8.21.38-lts"),
        ("24.10.4.191-lts-34d87c12a", "22.8.21.38"),
        ("24.10.4.191", "22.8.21.38-lts-34d87c12a"),
    ],
)
def test_match_str_ch_version(version: str, min_version: str) -> None:
    match_str_ch_version(version, min_version)
