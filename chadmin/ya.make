OWNER(g:mdb)

PY3_PROGRAM(chadmin)

STYLE_PYTHON()

ALL_PY_SRCS(
    RECURSIVE
    NAMESPACE
    cloud.mdb.clickhouse.tools.chadmin
)

PY_MAIN(cloud.mdb.clickhouse.tools.chadmin.chadmin:main)

PEERDIR(
    cloud/mdb/cli/common
    cloud/mdb/clickhouse/tools/common
    contrib/python/click
    contrib/python/humanfriendly
    contrib/python/kazoo
    contrib/python/PyYAML
)

END()
