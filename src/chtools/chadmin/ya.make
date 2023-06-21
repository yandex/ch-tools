OWNER(g:mdb)

PY3_PROGRAM(chadmin)

STYLE_PYTHON()

ALL_PY_SRCS(
    RECURSIVE
    NAMESPACE
    chtools.chadmin
)

PY_MAIN(chtools.chadmin.chadmin_cli:main)

PEERDIR(
    cloud/mdb/clickhouse/tools/src/chtools/common
    contrib/python/PyYAML
    contrib/python/boto3
    contrib/python/click
    contrib/python/humanfriendly
    contrib/python/kazoo
    contrib/python/lxml
    contrib/python/setuptools
    contrib/python/typing-extensions
)

END()
