OWNER(g:mdb)

PY3_PROGRAM(ch-resetup)

PY_SRCS(
	MAIN main.py
)

PEERDIR(
    contrib/python/kazoo
    contrib/python/requests
    contrib/python/tenacity
)

END()
