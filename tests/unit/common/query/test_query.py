from ch_tools.common.clickhouse.client.query import Query

query_1 = Query(
    "SELECT * FROM users WHERE name = {{name}} AND password = {password}",
    {"password": "123"},
)
query_2 = Query("SELECT * FROM users WHERE name = {name}", {})
query_3 = Query("SELECT * FROM users WHERE password = {password}", {"password": "123"})


def test_for_execute() -> None:
    assert (
        query_1.for_execute()
        == "SELECT * FROM users WHERE name = {name} AND password = 123"
    )
    assert query_2.for_execute() == "SELECT * FROM users WHERE name = {name}"
    assert query_3.for_execute() == "SELECT * FROM users WHERE password = 123"


def test_str() -> None:
    assert (
        str(query_1) == "SELECT * FROM users WHERE name = {name} AND password = *****"
    )
    assert str(query_2) == "SELECT * FROM users WHERE name = {name}"
    assert str(query_3) == "SELECT * FROM users WHERE password = *****"


def test_repr() -> None:
    assert (
        repr(query_1)
        == "Query(value='SELECT * FROM users WHERE name = {name} AND password = *****', sensitive_args={'password': '*****'})"
    )
    assert (
        repr(query_2)
        == "Query(value='SELECT * FROM users WHERE name = {name}', sensitive_args={})"
    )
    assert (
        repr(query_3)
        == "Query(value='SELECT * FROM users WHERE password = *****', sensitive_args={'password': '*****'})"
    )


def test_eq_and_hash() -> None:
    query_2 = Query(query_1.value, query_1.sensitive_args)
    assert query_1 == query_2
    assert hash(query_1) == hash(query_2)


def test_add() -> None:
    added_query = query_1 + " LIMIT 10"
    assert (
        str(added_query)
        == "SELECT * FROM users WHERE name = {name} AND password = ***** LIMIT 10"
    )
