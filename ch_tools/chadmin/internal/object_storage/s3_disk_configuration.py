from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from lxml import etree  # type: ignore[import]
from typing_extensions import Self

BUCKET_NAME_PREFIX = "cloud-storage-"


@dataclass
class S3DiskConfiguration:
    name: str
    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    bucket_name: str
    prefix: str

    @classmethod
    def from_config(cls, config_path: Path, disk_name: str) -> Self:
        config = etree.parse(config_path)
        disk = config.find(f"/storage_configuration/disks/{disk_name}")

        disk_type = disk.find("type").text
        if disk_type != "s3":
            raise TypeError(f"Unsupported object storage type {disk_type}")

        access_key_id = disk.find("access_key_id").text
        secret_access_key = disk.find("secret_access_key").text
        endpoint: str = disk.find("endpoint").text

        _host, bucket_name, prefix, endpoint_url = _parse_endpoint(endpoint)

        return cls(
            name=disk_name,
            endpoint_url=endpoint_url,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            bucket_name=bucket_name,
            prefix=prefix,
        )


def _parse_endpoint(endpoint: str) -> tuple:
    """
    Parse both virtual and path style S3 url.
    """
    url = urlparse(endpoint)
    if url.hostname is None:
        raise ValueError(f"Incorrect endpoint format {endpoint}")

    path = url.path.removeprefix("/")
    if url.hostname.startswith(BUCKET_NAME_PREFIX):
        # virtual addressing style
        bucket_name, host = url.hostname.split(".", maxsplit=1)
        prefix = path
    else:
        # path addressing style
        host = url.hostname
        bucket_name, prefix = path.split("/", maxsplit=1)
        if not bucket_name.startswith(BUCKET_NAME_PREFIX):
            raise ValueError(
                f"Unexpected bucket name `{bucket_name}`. Parser expects `{BUCKET_NAME_PREFIX}` prefix"
            )

    endpoint_url = "{}://{}{}".format(
        url.scheme, host, f":{url.port}" if url.port else ""
    )

    return host, bucket_name, prefix, endpoint_url
