OWNER(g:mdb)

PY3_PROGRAM(zero_copy_schema_converter)

STYLE_PYTHON()

PY_SRCS(
    MAIN
    zero_copy_schema_converter.py
)

PEERDIR(
    contrib/python/kazoo
)

END()
