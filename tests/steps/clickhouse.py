"""
Steps for interacting with ClickHouse DBMS.
"""

from behave import given, then, when
from hamcrest import assert_that, equal_to
from modules.clickhouse import (
    check_table_exists_in_uuid_dir,
    execute_query,
    get_all_user_data,
    get_all_user_schemas,
    get_response,
    ping,
)
from modules.docker import get_container
from tenacity import retry, stop_after_attempt, wait_fixed


@given("a working clickhouse on {node:w}")
@then("a clickhouse will be worked on {node:w}")
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

    if not hasattr(context, "uuid_to_table"):
        context.uuid_to_table = {}
    context.uuid_to_table[table] = response


@then("check {disk} disk contains table {table} data in {node:w}")
def step_check_disk_contains_table_data(context, disk, table, node):
    """
    Check that disk contains table data (using table uuid that saved in step_save_table_uuid)
    """
    table_exists = check_table_exists_in_uuid_dir(context, table, disk, node)

    assert_that(
        table_exists,
        f"table {table} not exists on disk {disk}",
    )


@then("check table {table} not exists on {disk} disk in {node:w}")
def step_check_table_not_exists_on_disk(context, table, disk, node):
    """
    Check that table not exists on disk (using table uuid that saved in step_save_table_uuid)
    """
    table_exists = check_table_exists_in_uuid_dir(context, table, disk, node)

    assert_that(
        not table_exists,
        f"table {table} exists on disk {disk}",
    )


@given(
    "populated clickhouse with {count:d} replicated tables on {node:w} with {database_name} database and {table_prefix} prefix"
)
def step_populate_with_replicated_tables(
    context, count, node, database_name, table_prefix
):
    """
    Creates <count> number of replicated tables: database_name.table_prefix
    """

    execute_query(
        context,
        node,
        ("DROP DATABASE IF EXISTS %s ON CLUSTER '{cluster}'" % database_name),
    )

    execute_query(
        context, node, ("CREATE DATABASE %s ON CLUSTER '{cluster}'" % database_name)
    )

    for ind in range(0, count):
        table_name = f"{database_name}.{table_prefix}{ind}"
        query = (
            "CREATE TABLE %s ON CLUSTER '{cluster}' (n Int32) ENGINE = ReplicatedMergeTree('/%s/%s','{replica}') PARTITION BY n ORDER BY n;"
            % (table_name, database_name, table_name)
        )
        execute_query(context, node, query)


@then("{count:d} readonly replicas on {node:w}")
def step_check_number_ro_replicas(context, count, node):
    query = "SELECT count() FROM system.replicas WHERE is_readonly=1"

    ret_code, response = get_response(context, node, query)
    assert_that(response, equal_to(str(count)))
