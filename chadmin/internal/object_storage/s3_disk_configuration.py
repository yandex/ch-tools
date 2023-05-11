from dataclasses import dataclass
from pathlib import Path
from typing import Self
from urllib.parse import urlparse

from lxml import etree  # type: ignore[import]


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
        disk = config.find(f'/storage_configuration/disks/{disk_name}')

        disk_type = disk.find('type').text
        if disk_type != 's3':
            raise TypeError(f'Unsupported object storage type {disk_type}')

        access_key_id = disk.find('access_key_id').text
        secret_access_key = disk.find('secret_access_key').text
        endpoint: str = disk.find('endpoint').text

        url = urlparse(endpoint)
        if url.hostname is None:
            raise ValueError(f'Incorrect endpoint format {endpoint}')
        bucket_name, host = url.hostname.split('.', maxsplit=1)
        prefix = url.path.removeprefix('/')
        endpoint_url = '{}://{}{}'.format(url.scheme, host, f':{url.port}' if url.port else '')

        return cls(
            name=disk_name,
            endpoint_url=endpoint_url,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            bucket_name=bucket_name,
            prefix=prefix,
        )
