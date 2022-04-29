OWNER(g:mdb)

PY3_PROGRAM(ch-diagnostics)

STYLE_PYTHON()

PY_SRCS(
    MAIN
    main.py
)

PEERDIR(
    cloud/mdb/clickhouse/tools/common
    contrib/python/humanfriendly
    contrib/python/PyYAML
)

END()
