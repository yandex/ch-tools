OWNER(g:mdb)

PY3_PROGRAM(ch-s3-credentials)

STYLE_PYTHON()

PY_SRCS(
    MAIN
    main.py
)

PEERDIR(
    cloud/mdb/clickhouse/tools/common
    contrib/python/requests
)

END()
