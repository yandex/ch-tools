from dataclasses import dataclass
from urllib.parse import urlparse

# YC specific value for sanity checking
# TODO: pass bucket, prefix, endpoint as arguments to cli for versatility
BUCKET_NAME_PREFIX = "cloud-storage-"


@dataclass
class S3DiskConfiguration:
    name: str
    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    bucket_name: str
    prefix: str


class ClickhouseStorageConfiguration:
    """
    Storage configuration section of ClickHouse server config.
    """

    def __init__(self, config: dict) -> None:
        self._config = config

    def has_disk(self, name: str) -> bool:
        return name in self._config.get("disks", {})

    def s3_disk_configuaration(self, name: str) -> S3DiskConfiguration:
        if not self.has_disk(name):
            raise RuntimeError(f"Config section for disk '{name}' is not found")

        disk = self._config["disks"][name]

        if disk["type"] != "s3":
            raise TypeError(f"Unsupported object storage type {disk['type']}")

        access_key_id = disk["access_key_id"]
        secret_access_key = disk["secret_access_key"]
        endpoint: str = disk["endpoint"]

        _host, bucket_name, prefix, endpoint_url = _parse_endpoint(endpoint)

        return S3DiskConfiguration(
            name=name,
            endpoint_url=endpoint_url,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            bucket_name=bucket_name,
            prefix=prefix,
        )


def _parse_endpoint(endpoint: str) -> tuple:
    """
    Parse both virtual and path style S3 endpoints url.
    """
    url = urlparse(endpoint)
    if url.hostname is None:
        raise ValueError(f"Incorrect endpoint format {endpoint}")

    path = url.path[1:] if url.path.startswith("/") else url.path
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
