OWNER(g:mdb)

PY3_PROGRAM(ch-resetup)

STYLE_PYTHON()

PY_SRCS(
    MAIN
    main.py
)

PEERDIR(
    cloud/mdb/clickhouse/tools/common
)

END()
