OWNER(g:mdb)

PY3_PROGRAM(ch-monitoring)

ALL_PY_SRCS(RECURSIVE NAMESPACE cloud.mdb.clickhouse.tools.monrun_checks)

PY_MAIN(cloud.mdb.clickhouse.tools.monrun_checks.main:main)


PEERDIR(
    cloud/mdb/clickhouse/tools/common

    contrib/python/click
    contrib/python/requests
    contrib/python/PyYAML
    contrib/python/psutil
    contrib/python/pyOpenSSL
    contrib/python/tabulate
    cloud/mdb/clickhouse/tools/common
)

END()
