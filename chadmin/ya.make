OWNER(g:mdb)

PY3_PROGRAM(chadmin)

PY_SRCS(
	MAIN chadmin.py
    cli/__init__.py
    cli/async_metrics.py
    cli/databases.py
    cli/dictionaries.py
    cli/events.py
    cli/functions.py
    cli/macros.py
    cli/metrics.py
    cli/mutations.py
    cli/part_log.py
    cli/partitions.py
    cli/process.py
    cli/query_log.py
    cli/replication_queue.py
    cli/settings.py
    cli/stack_trace.py
    cli/system.py
    cli/tables.py
    cli/thread_log.py
    cli/zookeeper.py
)

PEERDIR(
    cloud/mdb/cli/common
    cloud/mdb/clickhouse/tools/common

    contrib/python/click
    contrib/python/humanfriendly
    contrib/python/kazoo
    contrib/python/PyYAML
)

END()
