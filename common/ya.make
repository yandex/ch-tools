OWNER(g:mdb)

PY3_LIBRARY(ch-tools-common)

STYLE_PYTHON()

ALL_PY_SRCS(RECURSIVE)

PEERDIR(
    contrib/python/Jinja2
    contrib/python/PyYAML
    contrib/python/requests
    contrib/python/tenacity
    contrib/python/xmltodict
)

END()
