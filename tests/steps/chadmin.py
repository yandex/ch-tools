"""
Steps for interacting with chadmin.
"""

from behave import given, then, when
from hamcrest import assert_that, equal_to
from modules import clickhouse as ch_module
from modules import s3 as s3_module
from modules.chadmin import Chadmin
from modules.docker import get_container
from modules.typing import ContextT


@when("we execute chadmin create zk nodes on {node:w}")
def step_create_(context: ContextT, node: str) -> None:
    container = get_container(context, node)
    nodes = context.text.strip().split("\n")
    chadmin = Chadmin(container)

    for node in nodes:
        result = chadmin.create_zk_node(node)
        assert result.exit_code == 0, f" output:\n {result.output.decode().strip()}"


@when("we do hosts cleanup on {node} with fqdn {fqdn} and zk root {zk_root}")
def step_host_cleanup_with_zk_root(
    context: ContextT, node: str, fqdn: str, zk_root: str
) -> None:
    container = get_container(context, node)
    result = Chadmin(container).zk_cleanup(fqdn, zk_root)
    assert result.exit_code == 0, f" output:\n {result.output.decode().strip()}"


@when("we do hosts dry cleanup on {node} with fqdn {fqdn} and zk root {zk_root}")
def step_host_dry_cleanup_with_zk_root(
    context: ContextT, node: str, fqdn: str, zk_root: str
) -> None:
    container = get_container(context, node)
    result = Chadmin(container).zk_cleanup(fqdn, zk_root, dry_run=True)
    assert result.exit_code == 0, f" output:\n {result.output.decode().strip()}"


@when("we do hosts cleanup on {node} with fqdn {fqdn}")
def step_host_cleanup(context: ContextT, node: str, fqdn: str) -> None:
    container = get_container(context, node)
    result = Chadmin(container).zk_cleanup(fqdn, no_ch_config=False)
    assert result.exit_code == 0, f" output:\n {result.output.decode().strip()}"


@when(
    "we do table cleanup on {node} with fqdn {fqdn} from table with {zk_table_path} zookeeper path"
)
def step_table_cleanup(
    context: ContextT, node: str, fqdn: str, zk_table_path: str
) -> None:
    container = get_container(context, node)
    result = Chadmin(container).zk_cleanup_table(fqdn, zk_table_path)
    assert result.exit_code == 0, f" output:\n {result.output.decode().strip()}"


@then("the list of children on {node:w} for zk node {zk_node} is equal to")
def step_childen_list(context: ContextT, node: str, zk_node: str) -> None:
    container = get_container(context, node)
    result = Chadmin(container).zk_list(zk_node)
    assert_that(result.output.decode(), equal_to(context.text + "\n"))


@then("the list of children on {node:w} for zk node {zk_node} is empty")
def step_childen_list_empty(context: ContextT, node: str, zk_node: str) -> None:
    container = get_container(context, node)
    result = Chadmin(container).zk_list(zk_node)
    assert_that(result.output.decode(), equal_to("\n"))


@when("we delete zookeepers nodes {zk_nodes} on {node:w}")
def step_delete_command(context: ContextT, zk_nodes: str, node: str) -> None:
    container = get_container(context, node)
    result = Chadmin(container).zk_delete(zk_nodes)
    assert result.exit_code == 0, f" output:\n {result.output.decode().strip()}"


@then('dictionary "{dict_name}" returns expected values on {node:w}')
def step_check_dict_values(context: ContextT, dict_name: str, node: str) -> None:
    container = get_container(context, node)

    reload_result = container.exec_run(
        'clickhouse-client -q "SYSTEM RELOAD DICTIONARIES"'
    )
    assert reload_result.exit_code == 0, (
        f"SYSTEM RELOAD DICTIONARIES failed with exit code "
        f"{reload_result.exit_code}, output:\n{reload_result.output.decode().strip()}"
    )

    errors = []
    for row in context.table:
        identifier = row["id"]
        attr = row["attribute"]
        expected = row["expected"]

        query = f"SELECT dictGet('{dict_name}', '{attr}', {identifier})"
        result = container.exec_run(f'clickhouse-client -q "{query}"')

        if result.exit_code != 0:
            errors.append(f"\nQuery failed: {result.output.decode()}")
            continue

        actual = result.output.decode().strip()

        if actual != expected:
            errors.append(
                f"id={identifier}, attribute='{attr}': expected '{expected}', got '{actual}'"
            )

    if errors:
        raise AssertionError("\n".join(errors))


@given("we corrupt column {column} blobs of part of table {db}.{table} on {node:w}")
def step_corrupt_column_blobs(
    context: ContextT, column: str, db: str, table: str, node: str
) -> None:
    """Copy the first active part of *db*.*table* into detached/broken_<part>,
    then delete all S3 blobs referenced by files belonging to *column*.

    After this step ``context.broken_part_path`` is set to the path of the
    broken copy so that subsequent steps can pass it to ``chadmin``.
    """
    # S3 key prefix used by the object_storage disk (see config.xml)
    s3_prefix = "data/cluster_id/shard_1"

    # 1. Find the active part path
    part_path = ch_module.get_part_path(context, node, db, table)

    # 2. Copy it to detached/broken_<part>
    broken_path = ch_module.detach_part_copy_as_broken(context, node, part_path)

    # 3. Find all files belonging to the column inside the broken copy
    col_files = ch_module.list_column_files(context, node, broken_path, column)
    assert col_files, f"No files found for column '{column}' in {broken_path}"

    # 4. For each file, parse S3 metadata and delete the referenced blobs
    s3_client = s3_module.S3Client(context)
    deleted = 0
    for file_path in col_files:
        keys = ch_module.read_s3_metadata_keys(context, node, file_path, s3_prefix)
        for key in keys:
            s3_client.delete_data(key)
            deleted += 1

    assert deleted > 0, (
        f"No S3 blobs were deleted for column '{column}' — "
        f"check that the files are S3 metadata files"
    )

    # 5. Store the broken part path for use in subsequent steps
    context.broken_part_path = broken_path

    # Also write it to a file inside the container so bash commands in the
    # feature can reference it via $(cat /tmp/broken_part_path)
    _write_broken_part_path(context, node, broken_path)


@given("we copy part of table {db}.{table} to detached on {node:w}")
def step_copy_part_to_detached(
    context: ContextT, db: str, table: str, node: str
) -> None:
    """Copy the first active part of *db*.*table* into detached/broken_<part>
    **without** deleting any S3 blobs.  Used to test the "all blobs healthy"
    recovery path.

    After this step ``context.broken_part_path`` is set and
    ``/tmp/broken_part_path`` is written inside the container.
    """
    part_path = ch_module.get_part_path(context, node, db, table)
    broken_path = ch_module.detach_part_copy_as_broken(context, node, part_path)
    context.broken_part_path = broken_path
    _write_broken_part_path(context, node, broken_path)


@given("we corrupt data.bin blobs of part of table {db}.{table} on {node:w}")
def step_corrupt_data_bin_blobs(
    context: ContextT, db: str, table: str, node: str
) -> None:
    """Copy the first active part of *db*.*table* into detached/broken_<part>,
    then delete all S3 blobs referenced by the ``data.bin`` file (Compact format).

    After this step ``context.broken_part_path`` is set to the path of the
    broken copy so that subsequent steps can pass it to ``chadmin``.
    """
    # S3 key prefix used by the object_storage disk (see config.xml)
    s3_prefix = "data/cluster_id/shard_1"

    # 1. Find the active part path
    part_path = ch_module.get_part_path(context, node, db, table)

    # 2. Copy it to detached/broken_<part>
    broken_path = ch_module.detach_part_copy_as_broken(context, node, part_path)

    # 3. Find the data.bin file inside the broken copy
    data_bin_path = f"{broken_path}/data.bin"

    # 4. Parse S3 metadata and delete the referenced blobs
    s3_client = s3_module.S3Client(context)
    keys = ch_module.read_s3_metadata_keys(context, node, data_bin_path, s3_prefix)
    assert keys, f"No S3 metadata found in {data_bin_path} — check that it is a Compact part"

    deleted = 0
    for key in keys:
        s3_client.delete_data(key)
        deleted += 1

    assert deleted > 0, (
        f"No S3 blobs were deleted for data.bin — "
        f"check that the file is an S3 metadata file"
    )

    # 5. Store the broken part path for use in subsequent steps
    context.broken_part_path = broken_path

    # Also write it to a file inside the container so bash commands in the
    # feature can reference it via $(cat /tmp/broken_part_path)
    _write_broken_part_path(context, node, broken_path)


def _write_broken_part_path(context: ContextT, node: str, broken_path: str) -> None:
    """Write *broken_path* to ``/tmp/broken_part_path`` inside the container."""
    container = get_container(context, node)
    result = container.exec_run(
        ["bash", "-c", f"printf '%s' '{broken_path}' > /tmp/broken_part_path"],
        user="root",
    )
    assert (
        result.exit_code == 0
    ), f"Failed to write broken_part_path to container: {result.output.decode()}"
