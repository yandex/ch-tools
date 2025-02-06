import pytest

from ch_tools.chadmin.internal.system import validate_version


@pytest.mark.parametrize(
    "version",
    [
        "22.8.21.38",
        "24.10.4.191",
    ],
)
def test_validate_version(version):
    validate_version(version)
