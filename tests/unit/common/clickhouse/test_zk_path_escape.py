import pytest

from ch_tools.chadmin.internal.zookeeper import escape_for_zookeeper

# type: ignore


@pytest.mark.parametrize(
    "hostname, result",
    [
        pytest.param(
            "zone-hostname.database.urs.net",
            "zone%2Dhostname%2Edatabase%2Eurs%2Enet",
        ),
    ],
)
def test_config(hostname, result):

    assert escape_for_zookeeper(hostname) == result
