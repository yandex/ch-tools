"""
Steps for interacting with ClickHouse DBMS.
"""

from behave import given, then, when
from hamcrest import assert_that, equal_to
from modules.clickhouse import (
    execute_query,
    get_all_user_data,
    get_all_user_schemas,
    get_response,
    ping,
)
from modules.docker import get_container
from tenacity import retry, stop_after_attempt, wait_fixed

from ch_tools.chadmin.internal.clickhouse_disks import (
    CLICKHOUSE_PATH,
    OBJECT_STORAGE_DISK_TYPES,
    S3_PATH,
)


@given("a working clickhouse on {node:w}")
@retry(wait=wait_fixed(0.5), stop=stop_after_attempt(40))
def step_wait_for_clickhouse_alive(context, node):
    """
    Wait until clickhouse is ready to accept incoming requests.
    """
    ping(context, node)


@given("we have executed query on {node:w}")
@when("we execute query on {node:w}")
def step_clickhouse_query(context, node):
    context.ret_code, context.response = get_response(context, node, context.text)


@given("we have executed queries on {node:w}")
@when("we execute queries on {node:w}")
def step_clickhouse_queries(context, node):
    queries = []
    for string in context.text.split(";"):
        string = string.strip()
        if string:
            queries.append(string)

    for query in queries:
        execute_query(context, node, query)


@given("we get response code {code:d}")
@then("we get response code {code:d}")
def step_clickhouse_response(context, code):
    assert_that(code, equal_to(context.ret_code))


@then("{node1:w} has the same schema as {node2:w}")
def step_has_same_schema(context, node1, node2):
    def _get_schema(node):
        return get_all_user_schemas(context, node)

    assert_that(_get_schema(node1), equal_to(_get_schema(node2)))


@then("{node1:w} has the same data as {node2:w}")
def step_same_clickhouse_data(context, node1, node2):
    def _get_data(node):
        _, data = get_all_user_data(context, node)
        return data

    assert_that(_get_data(node1), equal_to(_get_data(node2)))


@then("there are no unfinished dll queries on {node:w}")
def step_check_unfinished_ddl(context, node):
    query = "SELECT count(*) FROM system.distributed_ddl_queue WHERE status!='Finished'"
    ret_code, response = get_response(context, node, query)
    assert_that(response, equal_to("0"))


@when("we put the  clickhouse config to path {path} with restarting on {node:w}")
def step_put_config(context, path, node):
    config = context.text
    container = get_container(context, node)
    result = container.exec_run(
        ["bash", "-c", f'echo -e " {config} " > {path}'], user="root"
    )
    assert_that(result.exit_code, equal_to(0))

    result = container.exec_run(
        ["bash", "-c", "supervisorctl restart clickhouse-server"], user="root"
    )
    assert_that(result.exit_code, equal_to(0))


@then("save uuid table {table} in context on {node:w}")
def step_save_table_uuid(context, table, node):
    query = f"SELECT uuid FROM system.tables WHERE name='{table}'"
    ret_code, response = get_response(context, node, query)
    assert 200 == ret_code

    if not hasattr(context, "table_uuid"):
        context.uuid_to_table = {}
    context.uuid_to_table[table] = response


def _get_table_dir_from_disk(context, table, disk, node):
    if disk in OBJECT_STORAGE_DISK_TYPES:
        path = S3_PATH
    else:
        path = CLICKHOUSE_PATH

    path = path + "/store/"

    table_uuid = context.uuid_to_table.get(table, None)

    assert_that(table_uuid is not None, f"not found saved uuid for table {table}")
    assert_that(len(table_uuid) > 0, f"found empty uuid for table {table}")

    container = get_container(context, node)

    table_path = path + table_uuid[:3]
    return container.exec_run(
        ["bash", "-c", f"ls {table_path}"], user="root"
    ).output.decode()


@then("check {disk} disk contains table {table} data in {node:w}")
def step_check_disk_contains_table_data(context, disk, table, node):
    """
    Check that disk contains table data (using table uuid that saved in step_save_table_uuid)
    """
    output = _get_table_dir_from_disk(context, table, disk, node)
    assert_that(0 != len(output), f"table {table} not exists on disk {disk}")


@then("check table {table} not exists on {disk} disk in {node:w}")
def step_check_table_not_exists_on_disk(context, table, disk, node):
    """
    Check that table not exists on disk (using table uuid that saved in step_save_table_uuid)
    """
    output = _get_table_dir_from_disk(context, table, disk, node)
    assert_that(0 == len(output), f"table {table} exists on disk {disk}")
