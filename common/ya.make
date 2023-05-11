OWNER(g:mdb)

PY3_LIBRARY(ch-tools-common)

STYLE_PYTHON()

ALL_PY_SRCS(
    NAMESPACE common
    RECURSIVE
)

PEERDIR(
    contrib/python/Jinja2
    contrib/python/PyYAML
    contrib/python/Pygments
    contrib/python/click
    contrib/python/cloup
    contrib/python/deepdiff
    contrib/python/humanfriendly
    contrib/python/python-dateutil
    contrib/python/requests
    contrib/python/tabulate
    contrib/python/tenacity
    contrib/python/termcolor
    contrib/python/tqdm
    contrib/python/xmltodict
)

END()
