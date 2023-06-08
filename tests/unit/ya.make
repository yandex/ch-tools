OWNER(g:mdb)

PY3TEST()

STYLE_PYTHON()

ALL_PYTEST_SRCS(
    RECURSIVE
    ONLY_TEST_FILES
)

PEERDIR(
    cloud/mdb/clickhouse/tools/src/chtools/common
    contrib/python/PyHamcrest
    contrib/python/pytest
)

END()
