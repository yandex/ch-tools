OWNER(g:mdb)

PY3_LIBRARY(ch-tools-common)

PY_SRCS(
    clickhouse.py
    dbaas.py
)

PEERDIR(
    contrib/python/requests
    contrib/python/tenacity
    contrib/python/xmltodict
)

END()
