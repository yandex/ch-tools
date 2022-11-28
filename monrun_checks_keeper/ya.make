OWNER(g:mdb)

PY3_PROGRAM(keeper-monitoring)

STYLE_PYTHON()

ALL_PY_SRCS(
    RECURSIVE
)

PY_MAIN(cloud.mdb.clickhouse.tools.monrun_checks_keeper.main:main)

PEERDIR(
    cloud/mdb/clickhouse/tools/common
    contrib/python/click
    contrib/python/tabulate
    contrib/python/kazoo
)

END()
