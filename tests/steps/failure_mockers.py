"""
Steps for interacting with ClickHouse DBMS.
"""

import os
import shlex

from behave import when
from hamcrest import assert_that, equal_to
from modules import s3
from modules.clickhouse import execute_query
from modules.docker import get_container
from modules.steps import get_step_data
from modules.typing import ContextT

from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    S3ObjectLocalMetaData,
)


@when("we remove key from s3 for partitions database {database} on {node:w}")
def step_remove_keys_from_s3_for_partition(
    context: ContextT, database: str, node: str
) -> None:
    data = get_step_data(context)
    keys_to_remove = []

    # Get the list of keys in s3 to broke specified partitions.
    for database, database_info in data.items():
        for table, table_info in database_info.items():
            for partition in table_info:
                get_parts_info_query = f"SELECT name, partition, path  FROM system.parts where database='{database}' and table='{table}' and partition='{partition}'"
                # Get local path on disk to the single data part.
                part_local_path = execute_query(
                    context, node, get_parts_info_query, format_="JSONCompact"
                )["data"][0][2]
                keys_to_remove.append(
                    get_s3_object_keys_for_part_file(
                        context, node, part_local_path, "columns.txt"
                    )[0]
                )

    s3_client = s3.S3Client(context)
    for key in keys_to_remove:
        s3_client.delete_data(key)


@when(
    "we remove S3 blobs for file {filename} from detached part "
    "{database}.{table} on {node:w}"
)
def step_remove_file_blobs_from_detached_part(
    context: ContextT,
    filename: str,
    database: str,
    table: str,
    node: str,
) -> None:
    detached_parts = execute_query(
        context,
        node,
        (
            "SELECT path FROM system.detached_parts "
            f"WHERE database='{database}' AND table='{table}'"
        ),
        format_="JSONCompact",
    )["data"]
    assert len(detached_parts) == 1, (
        f"Expected one detached part for {database}.{table}, "
        f"found {len(detached_parts)}"
    )

    remote_paths = get_s3_object_keys_for_part_file(
        context,
        node,
        detached_parts[0][0],
        filename,
    )
    s3_client = s3.S3Client(context)
    for remote_path in remote_paths:
        s3_client.delete_data(remote_path)
        assert not s3_client.path_exists(remote_path)


@when(
    "we make file {filename} reference a missing empty S3 object in detached part "
    "{database}.{table} on {node:w}"
)
def step_make_file_reference_missing_empty_s3_object(
    context: ContextT,
    filename: str,
    database: str,
    table: str,
    node: str,
) -> None:
    detached_parts = execute_query(
        context,
        node,
        (
            "SELECT path FROM system.detached_parts "
            f"WHERE database='{database}' AND table='{table}'"
        ),
        format_="JSONCompact",
    )["data"]
    assert len(detached_parts) == 1, (
        f"Expected one detached part for {database}.{table}, "
        f"found {len(detached_parts)}"
    )

    part_path = detached_parts[0][0]
    logical_path = os.path.join(part_path, filename)
    container = get_container(context, node)
    read_result = container.exec_run(["cat", logical_path])
    assert read_result.exit_code == 0, read_result.output.decode(errors="replace")

    metadata = S3ObjectLocalMetaData.from_string(
        read_result.output.decode(encoding="latin-1")
    )
    assert metadata.total_size == 0, f"Expected {logical_path} to be empty"
    assert not metadata.objects, f"Expected {logical_path} to have no S3 objects"

    reference_path = os.path.join(part_path, "id.bin")
    reference_result = container.exec_run(["cat", reference_path])
    assert reference_result.exit_code == 0, reference_result.output.decode(
        errors="replace"
    )
    reference_metadata = S3ObjectLocalMetaData.from_string(
        reference_result.output.decode(encoding="latin-1")
    )
    assert len(reference_metadata.objects) == 1, (
        f"Expected one S3 object in {reference_path}, "
        f"found {len(reference_metadata.objects)}"
    )
    assert (
        reference_metadata.has_full_object_key() == metadata.has_full_object_key()
    ), f"Expected matching object key formats in {logical_path} and {reference_path}"

    reference_keys = get_s3_object_keys_for_part_file(
        context, node, part_path, "id.bin"
    )
    assert len(reference_keys) == 1, (
        f"Expected one remote S3 object for {reference_path}, "
        f"found {len(reference_keys)}"
    )
    reference_object = reference_metadata.objects[0]
    reference_key = reference_keys[0]
    assert reference_key.endswith(reference_object.key), (
        f"Expected remote S3 key {reference_key} to end with metadata key "
        f"{reference_object.key}"
    )

    missing_metadata_key = f"{reference_object.key}.missing-empty"
    missing_s3_key = f"{reference_key}.missing-empty"
    s3_client = s3.S3Client(context)
    assert not s3_client.path_exists(missing_s3_key)

    replacement = (
        f"{metadata.version}\n"
        f"1\t0\n"
        f"0\t{missing_metadata_key}\n"
        f"{metadata.ref_counter}\n"
        f"{int(metadata.read_only)}\n\n"
    )
    command = f"printf %s {shlex.quote(replacement)} > {shlex.quote(logical_path)}"
    write_result = container.exec_run(["bash", "-c", command], user="root")
    assert write_result.exit_code == 0, write_result.output.decode(errors="replace")


@when(
    "we remove s3 object for active part file {filename} from table {database}.{table} on {node:w}"
)
def step_remove_s3_object_for_active_part_file(
    context: ContextT, filename: str, database: str, table: str, node: str
) -> None:
    remove_s3_object_for_active_part_file(
        context, filename, database, table, node, part_index=0
    )


@when(
    "we remove s3 object for active part {part_index:d} file {filename} "
    "from table {database}.{table} on {node:w}"
)
def step_remove_s3_object_for_selected_active_part_file(
    context: ContextT,
    part_index: int,
    filename: str,
    database: str,
    table: str,
    node: str,
) -> None:
    remove_s3_object_for_active_part_file(
        context, filename, database, table, node, part_index
    )


def remove_s3_object_for_active_part_file(
    context: ContextT,
    filename: str,
    database: str,
    table: str,
    node: str,
    part_index: int,
) -> None:
    part_path = get_active_part_path(context, database, table, node, part_index)

    object_key = get_s3_object_keys_for_part_file(context, node, part_path, filename)[0]

    s3_client = s3.S3Client(context)
    s3_client.delete_data(object_key)
    assert not s3_client.path_exists(object_key)


@when(
    "we replace s3 objects for active part file {filename} from table "
    "{database}.{table} on {node:w} with content of size {size:d}"
)
def step_replace_s3_objects_for_active_part_file(
    context: ContextT,
    filename: str,
    database: str,
    table: str,
    node: str,
    size: int,
) -> None:
    part_path = get_active_part_path(context, database, table, node)
    object_keys = get_s3_object_keys_for_part_file(context, node, part_path, filename)
    s3_client = s3.S3Client(context)
    for object_key in object_keys:
        s3_client.upload_data(b"x" * size, object_key)
        assert s3_client.path_exists(object_key)


@when(
    "we make s3 objects for active part file {filename} unreadable from table "
    "{database}.{table} on {node:w}"
)
def step_make_s3_objects_for_active_part_file_unreadable(
    context: ContextT,
    filename: str,
    database: str,
    table: str,
    node: str,
) -> None:
    part_path = get_active_part_path(context, database, table, node)
    object_keys = get_s3_object_keys_for_part_file(context, node, part_path, filename)
    bucket = context.conf["s3"]["bucket"]
    minio_container = get_container(context, context.conf["s3"]["container"])
    for object_key in object_keys:
        object_path = os.path.join("/export", bucket, object_key.lstrip("/"))
        # Keep xl.meta intact so HEAD succeeds, but move the payload so
        # GetObject fails while reading the body.
        move_result = minio_container.exec_run(
            [
                "sh",
                "-c",
                (
                    'set -- "$1"/*/part.1; [ -f "$1" ] || exit 1; '
                    'for path in "$@"; do mv "$path" "$path.unreadable" || exit 1; done'
                ),
                "sh",
                object_path,
            ]
        )
        assert move_result.exit_code == 0, (
            f"Expected non-inline MinIO data for {object_key}: "
            f"{move_result.output.decode(errors='replace')}"
        )


def get_active_part_path(
    context: ContextT,
    database: str,
    table: str,
    node: str,
    part_index: int = 0,
) -> str:
    part_path_query = (
        "SELECT path FROM system.parts "
        f"WHERE database='{database}' AND table='{table}' AND active "
        f"ORDER BY name LIMIT 1 OFFSET {part_index}"
    )
    part_data = execute_query(context, node, part_path_query, format_="JSONCompact")[
        "data"
    ]
    if not part_data:
        raise AssertionError(
            f"No active part {part_index} found for table {database}.{table} "
            f"on node {node}"
        )
    return part_data[0][0]


def get_s3_object_keys_for_part_file(
    context: ContextT, node: str, part_path: str, filename: str
) -> list[str]:
    logical_path = os.path.join(part_path, filename)
    escaped_path = logical_path.replace("\\", "\\\\").replace("'", "\\'")
    object_key_query = (
        "SELECT remote_path FROM system.remote_data_paths "
        "WHERE disk_name='object_storage' "
        f"AND startsWith(concat(path, local_path), '{escaped_path}')"
    )
    object_key_data = execute_query(
        context, node, object_key_query, format_="JSONCompact"
    )["data"]
    if not object_key_data:
        raise AssertionError(
            f"No remote object path found for part file {filename} "
            f"at {part_path} on {node}"
        )
    return sorted({row[0] for row in object_key_data})


@when("we move parts as broken_on_start for table {database}.{table} on {node:w}")
def step_mark_parts_as_broken_on_start(
    context: ContextT, database: str, table: str, node: str
) -> None:
    part_list_query = f"SELECT name FROM system.parts WHERE database='{database}' and table='{table}' and active"

    for resp in execute_query(context, node, part_list_query, format_="JSONCompact")[
        "data"
    ]:
        detach_part = f"ALTER TABLE {database}.{table} DETACH PART '{resp[0]}'"
        execute_query(context, node, detach_part)

    broken_prefix = "broken-on-start_"
    detached_part_list_query = f"SELECT path FROM system.detached_parts WHERE database='{database}' and table='{table}'"
    container = get_container(context, node)

    for resp in execute_query(
        context, node, detached_part_list_query, format_="JSONCompact"
    )["data"]:
        path = resp[0]
        broken_path = path.split("/")
        broken_path[-1] = broken_prefix + broken_path[-1]
        broken_path = os.path.join("/", *broken_path)
        result = container.exec_run(
            ["bash", "-c", f"mv {path} {broken_path}"], user="root"
        )
        assert_that(result.exit_code, equal_to(0))
