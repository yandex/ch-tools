OWNER(g:mdb)

PY3_PROGRAM(ch-resetup)


PY_SRCS(
    main.py
)

PEERDIR(
    contrib/python/kazoo
    contrib/python/requests
    contrib/python/tenacity
)

END()
