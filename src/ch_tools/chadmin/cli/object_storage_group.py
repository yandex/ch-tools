import contextlib
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from gzip import GzipFile
from io import TextIOWrapper
from pathlib import Path
from typing import Dict, List, Optional

import click
from click import Context, group, option, pass_context

from ch_tools.chadmin.internal.object_storage import (
    ObjectSummary,
    S3DiskConfiguration,
    S3ObjectLocalMetaData,
    cleanup_s3_object_storage,
    collect_metadata,
    s3_object_storage_iterator,
)
from ch_tools.common.cli.parameters import TimeSpanParamType

STORAGE_POLICY_CONFIG_PATH = Path("/etc/clickhouse-server/config.d/storage_policy.xml")
# The guard interval is used for S3 objects for which metadata is not found.
# And for metadata for which object is not found in S3.
# These objects are not counted if their last modified time fall in the interval from the moment of starting analyzing.
DEFAULT_GUARD_INTERVAL = "24h"


def get_disk_metadata_paths(disk_name: str) -> List[Path]:
    return [
        Path(f"/var/lib/clickhouse/disks/{disk_name}/store"),  # Atomic database engine
        Path(f"/var/lib/clickhouse/disks/{disk_name}/data"),  # Ordinary database engine
        Path(f"/var/lib/clickhouse/disks/{disk_name}/shadow"),  # Backups
    ]


@group("object-storage")
@option(
    "-c",
    "--config",
    "config_path",
    type=click.Path(
        exists=True, file_okay=True, dir_okay=False, readable=True, path_type=Path
    ),
    default=STORAGE_POLICY_CONFIG_PATH,
    help="ClickHouse storage policy config",
)
@option(
    "-d",
    "--disk",
    "disk_name",
    default="object_storage",
    help="S3 disk name",
)
@pass_context
def object_storage_group(ctx: Context, config_path: Path, disk_name: str) -> None:
    """Commands to manage S3 objects and their metadata."""

    # Restrict excessive boto logging
    _set_boto_log_level(logging.WARNING)

    ctx.obj["disk_configuration"] = S3DiskConfiguration.from_config(
        config_path, disk_name
    )


@object_storage_group.command("list")
@option(
    "-o",
    "--orphaned",
    "orphaned",
    is_flag=True,
    default=False,
    help="List objects that are not referenced in the metadata",
)
@option(
    "-p",
    "--object-name-prefix",
    "object_name_prefix",
    default="",
    help="Additional prefix of object name using for listing",
)
@option(
    "-l",
    "--limit",
    "limit",
    type=int,
    help="Return at most this many objects",
)
@option(
    "-f",
    "--dump-file",
    "dump_file",
    type=click.Path(path_type=Path),
    help="Dump result to the file instead of STDOUT",
)
@option(
    "-c",
    "--compressed",
    "compressed",
    is_flag=True,
    help="Compress an output using GZIP format",
)
@option(
    "-q",
    "--quiet",
    "quiet",
    is_flag=True,
    help="Output only newline delimited object keys",
)
@option(
    "-g",
    "--guard-interval",
    "guard_interval",
    default=DEFAULT_GUARD_INTERVAL,
    type=TimeSpanParamType(),
    help=(
        "Guard interval in human-friendly format."
        "Objects with a modification time falling within it from the now are not considered orphaned"
    ),
)
@pass_context
def list_objects(
    ctx: Context,
    orphaned: bool,
    object_name_prefix: str,
    dump_file: Optional[Path],
    compressed: bool,
    quiet: bool,
    guard_interval: timedelta,
    limit: Optional[int],
) -> None:
    """
    List S3 objects.
    """
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]
    pivot_time = datetime.now(timezone.utc) - guard_interval
    counter = 0

    with dump_writer(compressed, dump_file) as writer:
        object_key_to_metadata = collect_metadata(
            get_disk_metadata_paths(disk_conf.name)
        )
        for name, obj in s3_object_storage_iterator(disk_conf, object_name_prefix):
            if limit is not None and counter >= limit:
                break

            metadata = object_key_to_metadata.get(name)

            if orphaned and (metadata or obj.last_modified > pivot_time):
                continue
            if not orphaned and not metadata:
                continue

            writer.write(_get_dump_line(obj, metadata, quiet))
            counter += 1


@object_storage_group.command("clean")
@option(
    "-f",
    "--file",
    "file",
    type=click.File("rb"),
    help="File containing S3 object keys delimited by newlines",
    default=sys.stdin.buffer,
    show_default="STDIN",
)
@option(
    "-c",
    "--compressed",
    "compressed",
    is_flag=True,
    help="Input stream is compressed using GZIP format",
)
@pass_context
def clean_object_storage(ctx, file, compressed):
    """
    Clean up needless S3 objects.
    """
    disk_conf: S3DiskConfiguration = ctx.obj["disk_configuration"]

    if compressed:
        file = GzipFile(fileobj=file)
    file = TextIOWrapper(file)

    lines_stripped = (
        line.rstrip() for line in file
    )  # lazily iterate over file stripping newline
    deleted = cleanup_s3_object_storage(disk_conf, lines_stripped)

    click.echo(f"Deleted {deleted} objects from bucket [{disk_conf.bucket_name}]")


@contextlib.contextmanager
def dump_writer(compressed, file_path=None):
    writer = open(file_path, "wb") if file_path is not None else sys.stdout.buffer
    try:
        yield GzipFile(mode="wb", fileobj=writer) if compressed else writer
    finally:
        if file_path is not None or compressed:
            writer.close()


def _set_boto_log_level(level: int) -> None:
    logging.getLogger("boto3").setLevel(level)
    logging.getLogger("botocore").setLevel(level)
    logging.getLogger("nose").setLevel(level)
    logging.getLogger("s3transfer").setLevel(level)
    logging.getLogger("urllib3").setLevel(level)


def _get_dump_line(
    obj: ObjectSummary,
    metadata_files: Optional[Dict[Path, S3ObjectLocalMetaData]],
    quiet: bool,
) -> bytes:
    if quiet:
        res = obj.key
    else:
        res = json.dumps(
            {
                "object": {
                    "key": obj.key,
                    "size": obj.size,
                    "last_modified": str(obj.last_modified),
                },
                "files": list(metadata_files) if metadata_files else [],
            },
            default=str,
        )
    return f"{res}\n".encode()
