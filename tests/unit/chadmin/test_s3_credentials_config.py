from pathlib import Path
from unittest.mock import MagicMock, patch
from xml.etree import ElementTree

import pytest
from click.testing import CliRunner

from ch_tools.chadmin.cli.s3_credentials_config_group import s3_credentials_config_group


def test_update_s3_credentials_with_multiple_endpoints(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "s3_credentials.xml"
    monkeypatch.setattr(
        "ch_tools.chadmin.cli.s3_credentials_config_group.CLICKHOUSE_S3_CREDENTIALS_CONFIG_PATH",
        str(config_path),
    )
    with patch(
        "ch_tools.chadmin.cli.s3_credentials_config_group._request_token",
        return_value=MagicMock(
            status_code=200,
            content=b'{"token_type": "Bearer", "access_token": "IAM_TOKEN"}',
        ),
    ) as request_token:
        result = CliRunner().invoke(
            s3_credentials_config_group,
            [
                "update",
                "--endpoint",
                "https://storage.yandexcloud.net",
                "--endpoint",
                "https://storage.pe.yandexcloud.net",
                "--endpoint",
                "https://storage.yandexcloud.net",
            ],
            obj={
                "config": {
                    "loguru": {"handlers": {}},
                    "clickhouse": {"version": "24.11"},
                }
            },
        )

    assert result.exit_code == 0, result.output
    request_token.assert_called_once()
    sections = ElementTree.parse(config_path).findall("s3/*")
    assert [section.tag for section in sections] == ["cloud_storage", "cloud_storage_1"]
    assert [section.findtext("endpoint") for section in sections] == [
        "https://storage.yandexcloud.net",
        "https://storage.pe.yandexcloud.net",
    ]
    assert [section.findtext("access_header") for section in sections] == [
        "X-YaCloud-SubjectToken: IAM_TOKEN",
        "X-YaCloud-SubjectToken: IAM_TOKEN",
    ]
