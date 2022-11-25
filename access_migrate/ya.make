OWNER(g:mdb)

PY3_PROGRAM(ch-access-migrate)

STYLE_PYTHON()

ALL_PY_SRCS(
    RECURSIVE
    NAMESPACE
    cloud.mdb.clickhouse.tools.access_migrate
)

PY_MAIN(cloud.mdb.clickhouse.tools.access_migrate.main:main)

PEERDIR(
    cloud/mdb/clickhouse/tools/common
    contrib/python/click
    contrib/python/kazoo
)

END()
