OWNER(g:mdb)

PY3_PROGRAM(ch-monitoring)

PY_SRCS(
    MAIN main.py
    result.py
    clickhouse_client.py
    clickhouse_info.py
    ch_replication_lag.py
    ch_system_queues.py
    ch_core_dumps.py
    ch_dist_tables.py
    ch_geobase.py
    ch_resetup_state.py
    ch_ro_replica.py
    ch_log_errors.py
    ch_ping.py
)

PEERDIR(
    contrib/python/click
    contrib/python/requests
    contrib/python/PyYAML
    contrib/python/psutil
)

END()
