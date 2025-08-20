from dataclasses import dataclass
from urllib.parse import urlparse

OBJECT_STORAGE_TYPES = ["s3", "hdfs", "azure_blob_storage", "local_blob_storage", "web"]


@dataclass
class ObjectStorageDiskConfiguration:
    name: str

    @staticmethod
    def _check_type(disk_config: dict, object_storage_type: str) -> None:
        # Since 24.1 there are two kinds of syntax in config
        type_in_config = (
            disk_config["object_storage_type"]
            if "object_storage_type" in disk_config
            else disk_config["type"]
        )

        if type_in_config not in OBJECT_STORAGE_TYPES:
            raise RuntimeError(
                f"Unsupported object storage type in config: '{type_in_config}'."
            )
        if object_storage_type != type_in_config:
            raise RuntimeError(
                f"Trying to load config for object storage type '{object_storage_type}', but actual type is '{type_in_config}'."
            )

    @staticmethod
    def _get_disk_config(
        config: "ClickhouseStorageConfiguration", name: str, object_storage_type: str
    ) -> dict:
        disk_config = config.get_disk_config(name)
        ObjectStorageDiskConfiguration._check_type(disk_config, object_storage_type)
        return disk_config


@dataclass
class S3DiskConfiguration(ObjectStorageDiskConfiguration):
    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    bucket_name: str
    prefix: str
    OBJECT_STORAGE_TYPE = "s3"

    @staticmethod
    def from_config(
        config: "ClickhouseStorageConfiguration", name: str, bucket_name_prefix: str
    ) -> "S3DiskConfiguration":
        disk = ObjectStorageDiskConfiguration._get_disk_config(
            config, name, S3DiskConfiguration.OBJECT_STORAGE_TYPE
        )

        access_key_id = disk["access_key_id"]
        secret_access_key = disk["secret_access_key"]
        endpoint: str = disk["endpoint"]

        _host, bucket_name, prefix, endpoint_url = S3DiskConfiguration._parse_endpoint(
            endpoint, bucket_name_prefix
        )

        return S3DiskConfiguration(
            name=name,
            endpoint_url=endpoint_url,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            bucket_name=bucket_name,
            prefix=prefix,
        )

    @staticmethod
    def _parse_endpoint(endpoint: str, bucket_name_prefix: str) -> tuple:
        """
        Parse both virtual and path style S3 endpoints url.
        """
        url = urlparse(endpoint)
        if url.hostname is None:
            raise ValueError(f"Incorrect endpoint format {endpoint}")

        path = url.path[1:] if url.path.startswith("/") else url.path
        if url.hostname.startswith(bucket_name_prefix):
            # virtual addressing style
            bucket_name, host = url.hostname.split(".", maxsplit=1)
            prefix = path
        else:
            # path addressing style
            host = url.hostname
            bucket_name, prefix = path.split("/", maxsplit=1)
            if not bucket_name.startswith(bucket_name_prefix):
                raise ValueError(
                    f"Unexpected bucket name `{bucket_name}`. Parser expects `{bucket_name_prefix}` prefix"
                )

        endpoint_url = f"{url.scheme}://{host}"
        if url.port:
            endpoint_url += f":{url.port}"

        return host, bucket_name, prefix, endpoint_url


@dataclass
class HDFSDiskConfiguration(ObjectStorageDiskConfiguration):
    endpoint: str
    OBJECT_STORAGE_TYPE = "hdfs"

    @staticmethod
    def from_config(
        config: "ClickhouseStorageConfiguration", name: str
    ) -> "HDFSDiskConfiguration":
        disk = ObjectStorageDiskConfiguration._get_disk_config(
            config, name, HDFSDiskConfiguration.OBJECT_STORAGE_TYPE
        )
        return HDFSDiskConfiguration(name, disk["endpoint"])


@dataclass
class AzureDiskConfiguration(ObjectStorageDiskConfiguration):
    storage_account_url: str
    container_name: str
    account_name: str
    account_key: str
    metadata_path: str
    cache_path: str
    OBJECT_STORAGE_TYPE = "azure_blob_storage"

    @staticmethod
    def from_config(
        config: "ClickhouseStorageConfiguration", name: str
    ) -> "AzureDiskConfiguration":
        disk = ObjectStorageDiskConfiguration._get_disk_config(
            config, name, AzureDiskConfiguration.OBJECT_STORAGE_TYPE
        )
        return AzureDiskConfiguration(
            name,
            disk["storage_account_url"],
            disk["container_name"],
            disk["account_name"],
            disk["account_key"],
            disk["metadata_path"],
            disk["cache_path"],
        )


@dataclass
class LocalDiskConfiguration(ObjectStorageDiskConfiguration):
    path: str
    OBJECT_STORAGE_TYPE = "local_blob_storage"

    @staticmethod
    def from_config(
        config: "ClickhouseStorageConfiguration", name: str
    ) -> "LocalDiskConfiguration":
        disk = ObjectStorageDiskConfiguration._get_disk_config(
            config, name, LocalDiskConfiguration.OBJECT_STORAGE_TYPE
        )
        return LocalDiskConfiguration(name, disk["path"])


@dataclass
class WebDiskConfiguration(ObjectStorageDiskConfiguration):
    endpoint: str
    OBJECT_STORAGE_TYPE = "web"

    @staticmethod
    def from_config(
        config: "ClickhouseStorageConfiguration", name: str
    ) -> "WebDiskConfiguration":
        disk = ObjectStorageDiskConfiguration._get_disk_config(
            config, name, WebDiskConfiguration.OBJECT_STORAGE_TYPE
        )
        return WebDiskConfiguration(name, disk["endpoint"])


class ClickhouseStorageConfiguration:
    """
    Storage configuration section of ClickHouse server config.
    """

    def __init__(self, config: dict) -> None:
        self._config = config

    def has_disk(self, name: str) -> bool:
        return name in self._config.get("disks", {})

    def get_disk_config(self, disk: str) -> dict:
        if not self.has_disk(disk):
            raise RuntimeError(f"Disk {disk} is not found in config.")
        return (self._config.get("disks", {}))[disk]
