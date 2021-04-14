OWNER(g:mdb)

PY3_PROGRAM(ch-diagnostics)

PY_SRCS(
	MAIN main.py
)

PEERDIR(
    cloud/mdb/clickhouse/tools/common

    contrib/python/humanfriendly
)

END()
