OWNER(g:mdb)

PY3_LIBRARY(ch-tools-common)

PY_SRCS(
    clickhouse.py
    dbaas.py
    utils.py
)

PEERDIR(
    contrib/python/Jinja2
    contrib/python/requests
    contrib/python/sqlparse
    contrib/python/tenacity
    contrib/python/xmltodict
)

END()
