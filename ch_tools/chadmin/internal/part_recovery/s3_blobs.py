"""
S3 blob health-check and download helpers for part recovery.

Uses boto3 via the credentials stored in :class:`S3DiskConfiguration`.
Parallel execution is done through :func:`execute_tasks_in_parallel`.
"""

import shutil
from enum import Enum
from pathlib import Path
from typing import Dict, List, Set

import boto3
from botocore.exceptions import ClientError

from ch_tools.chadmin.internal.part_recovery.classify import Decision, PartFile
from ch_tools.common import logging
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration
from ch_tools.common.process_pool import WorkerTask, execute_tasks_in_parallel


class BlobStatus(str, Enum):
    HEALTHY = "healthy"
    MISSING = "missing"


def _make_s3_client(disk_conf: S3DiskConfiguration) -> "boto3.client":
    return boto3.client(
        "s3",
        endpoint_url=disk_conf.endpoint_url,
        aws_access_key_id=disk_conf.access_key_id,
        aws_secret_access_key=disk_conf.secret_access_key,
    )


def head_blob(
    s3_client: "boto3.client",
    bucket: str,
    key: str,
) -> BlobStatus:
    """
    Check whether a single S3 blob exists.

    Returns :attr:`BlobStatus.HEALTHY` if the object is accessible,
    :attr:`BlobStatus.MISSING` on 404/NoSuchKey, and re-raises on any other
    error (403, 5xx, …) so that transient failures are not silently treated as
    data loss.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return BlobStatus.HEALTHY
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            return BlobStatus.MISSING
        raise


def check_single_blob(
    disk_conf: S3DiskConfiguration,
    key: str,
) -> bool:
    """Worker function: returns True if blob is healthy.

    *key* must be a **full** S3 key (prefix already included).
    A new boto3 client is created per call; this is intentional for
    thread-safety in :func:`execute_tasks_in_parallel`.
    """
    s3_client = _make_s3_client(disk_conf)
    status = head_blob(s3_client, disk_conf.bucket_name, key)
    return status == BlobStatus.HEALTHY


def check_blobs_parallel(
    disk_conf: S3DiskConfiguration,
    blob_keys: Set[str],
    threads: int = 16,
) -> Dict[str, bool]:
    """
    Check all S3 blobs in *blob_keys* in parallel.

    *blob_keys* must contain **full** S3 keys (prefix already included).

    Returns a mapping ``{full_blob_key: is_healthy}``.
    """
    if not blob_keys:
        return {}

    tasks: List[WorkerTask] = [
        WorkerTask(
            identifier=key,
            function=check_single_blob,
            kwargs={"disk_conf": disk_conf, "key": key},
        )
        for key in blob_keys
    ]

    logging.info("Checking {} S3 blobs with {} threads …", len(tasks), threads)
    results: Dict[str, bool] = execute_tasks_in_parallel(
        tasks, max_workers=threads, keep_going=False
    )
    return results


def collect_blob_keys(
    part_files: List[PartFile],
) -> Set[str]:
    """
    Collect all unique full S3 blob keys referenced by *part_files*.

    The keys stored in :attr:`PartFile.blob_keys` are already full keys
    (built by :func:`classify` using ``disk_conf``).
    """
    keys: Set[str] = set()
    for pf in part_files:
        keys.update(pf.blob_keys)
    return keys


def _download_single_blob(
    disk_conf: S3DiskConfiguration,
    key: str,
    dest_path: Path,
) -> None:
    """Worker function: download one blob to *dest_path*.

    *key* must be a **full** S3 key (prefix already included).
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    s3_client = _make_s3_client(disk_conf)
    logging.debug("Downloading s3://{}/{} → {}", disk_conf.bucket_name, key, dest_path)
    s3_client.download_file(disk_conf.bucket_name, key, str(dest_path))


def download_part_files(
    disk_conf: S3DiskConfiguration,
    part_files: List[PartFile],
    dest_dir: Path,
    threads: int = 16,
) -> None:
    """
    Download all healthy S3 blobs for *part_files* whose decision is DOWNLOAD,
    concatenating multi-blob files into a single local file under *dest_dir*.

    For each :class:`PartFile` with ``decision == Decision.DOWNLOAD`` and
    ``metadata is not None``, the blobs are downloaded to temporary paths and
    then concatenated (in order) into ``dest_dir / pf.name``.

    The keys in :attr:`PartFile.blob_keys` are already full S3 keys.
    """
    to_download = [
        pf
        for pf in part_files
        if pf.decision == Decision.DOWNLOAD and pf.metadata is not None
    ]

    if not to_download:
        return

    # Build a flat list of (blob_key, tmp_path, dest_file_path, blob_index) tasks
    tasks: List[WorkerTask] = []
    # Map: (pf.name, blob_index) → tmp_path
    tmp_paths: Dict[tuple, Path] = {}

    for pf in to_download:
        assert pf.metadata is not None
        for idx, key in enumerate(pf.blob_keys):
            tmp_path = dest_dir / f".tmp_{pf.name}.{idx}"
            tmp_paths[(pf.name, idx)] = tmp_path
            tasks.append(
                WorkerTask(
                    identifier=f"{pf.name}[{idx}]",
                    function=_download_single_blob,
                    kwargs={
                        "disk_conf": disk_conf,
                        "key": key,
                        "dest_path": tmp_path,
                    },
                )
            )

    logging.info("Downloading {} blobs for {} files …", len(tasks), len(to_download))
    execute_tasks_in_parallel(tasks, max_workers=threads, keep_going=False)

    # Concatenate blobs into final files using shutil.copyfileobj to avoid
    # loading entire blobs into memory (important for large parts).
    for pf in to_download:
        assert pf.metadata is not None
        dest_file = dest_dir / pf.name
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        with dest_file.open("wb") as out:
            for idx in range(len(pf.blob_keys)):
                tmp = tmp_paths[(pf.name, idx)]
                with tmp.open("rb") as src:
                    shutil.copyfileobj(src, out, length=65536)
                tmp.unlink()
        logging.debug("Assembled {}", dest_file)
