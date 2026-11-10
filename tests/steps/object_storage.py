"""Steps for object-storage cleanup with unavailable replicas."""

from behave import then, when
from modules.clickhouse import execute_query
from modules.s3 import S3Client
from modules.typing import ContextT


@when("we remember S3 objects of {table} on {node:w}")
def remember_table_objects(context: ContextT, table: str, node: str) -> None:
    database, name = table.split(".")
    rows = execute_query(
        context,
        node,
        f"""
        SELECT remote_path FROM system.remote_data_paths
        WHERE disk_name = 'object_storage'
          AND position(local_path, (SELECT toString(uuid) FROM system.tables
                                    WHERE database = '{database}' AND name = '{name}')) > 0
        """,
        format_="JSONCompact",
    )["data"]
    assert rows, f"No object-storage metadata found for {table}"
    if not hasattr(context, "remembered_s3_objects"):
        context.remembered_s3_objects = {}
    context.remembered_s3_objects[table] = [row[0] for row in rows]


@then("remembered S3 objects of {table} are {state:w}")
def check_table_objects(context: ContextT, table: str, state: str) -> None:
    assert state in ("present", "deleted")
    client = S3Client(context)
    for path in context.remembered_s3_objects[table]:
        assert client.path_exists(path) == (state == "present"), path
