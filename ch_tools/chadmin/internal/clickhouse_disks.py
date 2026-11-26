"""Helpers for executing non-interactive operations with ``clickhouse-disks``."""

import subprocess
from typing import List, Optional, Tuple

import xmltodict

from ch_tools.common import logging
from ch_tools.common.clickhouse.config import ClickhouseConfig

CLICKHOUSE_DISKS_TIMEOUT = 15 * 60
CLICKHOUSE_PATH = "/var/lib/clickhouse"
CLICKHOUSE_STORE_PATH = CLICKHOUSE_PATH + "/store"
CLICKHOUSE_DATA_PATH = CLICKHOUSE_PATH + "/data"
CLICKHOUSE_METADATA_PATH = CLICKHOUSE_PATH + "/metadata"
S3_PATH = CLICKHOUSE_PATH + "/disks/object_storage"
S3_METADATA_STORE_PATH = S3_PATH + "/store"

OBJECT_STORAGE_DISK_TYPES = ["s3", "object_storage", "ObjectStorage"]


def _quote_path(path: str) -> str:
    if "'" in path or "\n" in path or "\r" in path:
        raise ValueError(f"Unsupported clickhouse-disks path: {path!r}")
    return f"'{path}'"


def _query_has_error(stderr: bytes) -> bool:
    """Detect command errors hidden by the clickhouse-disks query interface."""
    return any(line.startswith(b"Error:") for line in stderr.splitlines())


class ClickHouseDiskClient:
    """Non-interactive wrapper around clickhouse-disks."""

    def __init__(
        self,
        disk: str,
        disk_config_path: Optional[str] = None,
        *,
        run_as_clickhouse: bool = True,
    ) -> None:
        self.disk = disk
        self.run_as_clickhouse = run_as_clickhouse
        self.disk_config_path = disk_config_path
        if self.disk_config_path is None and run_as_clickhouse:
            self.disk_config_path = make_ch_disks_config(disk)

    def _base_args(self) -> List[str]:
        args = (
            ["sudo", "-u", "clickhouse", "env", "HOME=/tmp"]
            if self.run_as_clickhouse
            else []
        )
        args.append("clickhouse-disks")
        if self.disk_config_path:
            args.extend(["-C", self.disk_config_path])
        args.extend(["--disk", self.disk])
        return args

    def _execute(
        self,
        command_args: List[str],
        *,
        stdin: Optional[bytes] = None,
        check: bool = True,
        dry_run: bool = False,
    ) -> subprocess.CompletedProcess[bytes]:
        args = [*self._base_args(), *command_args]
        logging.info("Run: {}", args)
        if dry_run:
            return subprocess.CompletedProcess(args, 0, b"", b"")

        try:
            proc = subprocess.run(
                args,
                input=stdin,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=CLICKHOUSE_DISKS_TIMEOUT,
            )
        except subprocess.TimeoutExpired as e:
            raise RuntimeError(
                "clickhouse-disks command timed out after "
                f"{CLICKHOUSE_DISKS_TIMEOUT} seconds: {args}"
            ) from e
        if proc.returncode == 0 and _query_has_error(proc.stderr):
            proc = subprocess.CompletedProcess(
                proc.args,
                1,
                proc.stdout,
                proc.stderr,
            )
        if check and proc.returncode:
            raise RuntimeError(
                f"clickhouse-disks command failed with code {proc.returncode}: "
                f"{proc.stderr.decode(errors='replace')}"
            )
        return proc

    def _run(self, command: str, stdin: Optional[bytes] = None) -> bytes:
        return self._execute(["--query", command], stdin=stdin).stdout

    def read(self, path: str) -> bytes:
        return self._run(f"read {_quote_path(path)}")

    def write(self, path: str, content: bytes) -> None:
        self._run(f"write {_quote_path(path)}", stdin=content)

    def copy(self, source: str, target: str) -> None:
        self._run(f"copy {_quote_path(source)} {_quote_path(target)}")

    def mkdir(self, path: str, parents: bool = False) -> None:
        suffix = " --parents" if parents else ""
        self._run(f"mkdir {_quote_path(path)}{suffix}")

    def remove(
        self,
        path: str,
        recursive: bool = False,
        *,
        dry_run: bool = False,
        check: bool = True,
    ) -> Tuple[int, bytes]:
        suffix = " --recursive" if recursive else ""
        command_args = ["--query", f"remove {_quote_path(path)}{suffix}"]

        proc = self._execute(command_args, check=check, dry_run=dry_run)
        logging.info(
            "clickhouse-disks remove command has finished: retcode {}, stderr: {}",
            proc.returncode,
            proc.stderr.decode(),
        )
        return (proc.returncode, proc.stderr)


def make_ch_disks_config(disk: str) -> str:
    disk_config = ClickhouseConfig.load().storage_configuration.get_disk_config(disk)
    disk_config_path = f"/tmp/chadmin-ch-disks-{disk}.xml"
    logging.info("Create a conf for {} disk: {}", disk, disk_config_path)
    with open(disk_config_path, "w", encoding="utf-8") as f:
        xmltodict.unparse(
            {
                "clickhouse": {
                    "storage_configuration": {"disks": {disk: disk_config}},
                }
            },
            f,
            pretty=True,
        )
    return disk_config_path


def remove_from_ch_disk(
    disk: str,
    path: str,
    disk_config_path: Optional[str] = None,
    dry_run: bool = False,
) -> Tuple[int, bytes]:
    """Compatibility wrapper for existing cleanup callers."""
    client = ClickHouseDiskClient(
        disk,
        disk_config_path,
        run_as_clickhouse=False,
    )
    return client.remove(
        path,
        recursive=True,
        dry_run=dry_run,
        check=False,
    )
