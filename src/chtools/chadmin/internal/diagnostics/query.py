SELECT_SYSTEM_TABLES = str.strip(
    # language=clickhouse
    r"""
SELECT name
FROM system.tables
WHERE database = 'system'
"""
)

SELECT_DATABASE_ENGINES = str.strip(
    # language=clickhouse
    r"""
SELECT
    engine,
    count() AS count
FROM system.databases
GROUP BY engine
"""
)

SELECT_DATABASES = str.strip(
    # language=clickhouse
    r"""
SELECT
    name,
    engine,
    tables,
    partitions,
    parts,
    formatReadableSize(bytes_on_disk) AS disk_size
FROM system.databases AS db
LEFT JOIN
(
    SELECT
        database,
        uniq(table) AS tables,
        uniq(table, partition) AS partitions,
        count() AS parts,
        sum(bytes_on_disk) AS bytes_on_disk
    FROM system.parts
    WHERE active
    GROUP BY database
) AS db_stats
ON db.name = db_stats.database
ORDER BY bytes_on_disk DESC
LIMIT 10
"""
)

SELECT_TABLE_ENGINES = str.strip(
    # language=clickhouse
    r"""
SELECT
    engine,
    count() AS count
FROM system.tables
WHERE database NOT IN ('system', 'INFORMATION_SCHEMA')
GROUP BY engine
"""
)

SELECT_DICTIONARIES = str.strip(
    # language=clickhouse
    r"""
SELECT
    source,
    type,
    status,
    count() AS count
FROM system.dictionaries
GROUP BY source, type, status
ORDER BY status DESC, source
"""
)

SELECT_ACCESS = str.strip(
    # language=clickhouse
    r"""
SHOW ACCESS
"""
)

SELECT_QUOTA_USAGE = str.strip(
    # language=clickhouse
    r"""
SHOW QUOTA
"""
)

SELECT_REPLICAS = str.strip(
    # language=clickhouse
    r"""
SELECT
    database,
    table,
    is_leader,
    is_readonly,
    absolute_delay,
    queue_size,
    inserts_in_queue,
    merges_in_queue
FROM system.replicas
ORDER BY absolute_delay DESC
LIMIT 10
"""
)

SELECT_REPLICATION_QUEUE = str.strip(
    # language=clickhouse
    r"""
SELECT
    database,
    table,
    replica_name,
    position,
    node_name,
    type,
    source_replica,
    parts_to_merge,
    new_part_name,
    create_time,
    required_quorum,
    is_detach,
    is_currently_executing,
    num_tries,
    last_attempt_time,
    last_exception,
    concat('time: ', toString(last_postpone_time), ', number: ', toString(num_postponed), ', reason: ', postpone_reason) AS postpone
FROM system.replication_queue
ORDER BY create_time
LIMIT 20
"""
)

SELECT_REPLICATED_FETCHES = str.strip(
    # language=clickhouse
    r"""
SELECT
    database,
    table,
    round(elapsed, 1) AS elapsed,
    round(100 * progress, 1) AS progress,
    partition_id,
    result_part_name,
    result_part_path,
    total_size_bytes_compressed,
    bytes_read_compressed,
    source_replica_path,
    source_replica_hostname,
    source_replica_port,
    interserver_scheme,
    to_detached,
    thread_id
FROM system.replicated_fetches
"""
)

SELECT_PARTS_PER_TABLE = str.strip(
    # language=clickhouse
    r"""
SELECT
    database,
    table,
    count() AS partitions,
    sum(part_count) AS parts,
    max(part_count) AS max_parts_per_partition
FROM
(
    SELECT
        database,
        table,
        partition,
        count() AS part_count
    FROM system.parts
    WHERE active
    GROUP BY database, table, partition
) AS partitions
GROUP BY database, table
ORDER BY max_parts_per_partition DESC
LIMIT 10
"""
)

SELECT_MERGES = str.strip(
    # language=clickhouse
    r"""
SELECT
    database,
    table,
    round(elapsed, 1) AS elapsed,
    round(100 * progress, 1) AS progress,
    is_mutation,
    partition_id,
    result_part_path,
    source_part_paths,
    num_parts,
    formatReadableSize(total_size_bytes_compressed) AS total_size_compressed,
    formatReadableSize(bytes_read_uncompressed) AS read_uncompressed,
    formatReadableSize(bytes_written_uncompressed) AS written_uncompressed,
    columns_written,
    formatReadableSize(memory_usage) AS memory_usage,
    thread_id
FROM system.merges
"""
)

SELECT_MUTATIONS = str.strip(
    # language=clickhouse
    r"""
SELECT
    database,
    table,
    mutation_id,
    command,
    create_time,
    parts_to_do_names,
    parts_to_do,
    is_done,
    latest_failed_part,
    latest_fail_time,
    latest_fail_reason
FROM system.mutations
WHERE NOT is_done
ORDER BY create_time DESC
"""
)

SELECT_RECENT_DATA_PARTS = str.strip(
    # language=clickhouse
    r"""
SELECT
    database,
    table,
    engine,
    partition_id,
    name,
    part_type,
    active,
    level,
    disk_name,
    path,
    marks,
    rows,
    bytes_on_disk,
    data_compressed_bytes,
    data_uncompressed_bytes,
    marks_bytes,
    modification_time,
    remove_time,
    refcount,
    is_frozen,
    min_date,
    max_date,
    min_time,
    max_time,
    min_block_number,
    max_block_number
FROM system.parts
WHERE modification_time > now() - INTERVAL 3 MINUTE
ORDER BY modification_time DESC
"""
)

SELECT_DETACHED_DATA_PARTS = str.strip(
    # language=clickhouse
    r"""
SELECT
    database,
    table,
    partition_id,
    name,
    disk,
    reason,
    min_block_number,
    max_block_number,
    level
FROM system.detached_parts
"""
)

SELECT_PROCESSES = str.strip(
    # language=clickhouse
    r"""
SELECT
    elapsed,
    query_id,
    {% if normalize_queries -%}
    normalizeQuery(query) AS normalized_query,
    {% else -%}
    query,
    {% endif -%}
    is_cancelled,
    concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) AS read,
    concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) AS written,
    formatReadableSize(memory_usage) AS "memory usage",
    user,
    multiIf(empty(client_name), http_user_agent, concat(client_name, ' ', toString(client_version_major), '.', toString(client_version_minor), '.', toString(client_version_patch))) AS client,
    {% if version_ge('21.3') -%}
    thread_ids,
    {% endif -%}
    {% if version_ge('21.8') -%}
    ProfileEvents,
    Settings
    {% else -%}
    ProfileEvents.Names,
    ProfileEvents.Values,
    Settings.Names,
    Settings.Values
    {% endif -%}
FROM system.processes
ORDER BY elapsed DESC
"""
)

SELECT_TOP_QUERIES_BY_DURATION = str.strip(
    # language=clickhouse
    r"""
SELECT
    type,
    query_start_time,
    query_duration_ms,
    query_id,
    {% if version_ge('21.3') -%}
    query_kind,
    {% endif -%}
    is_initial_query,
    {% if normalize_queries -%}
    normalizeQuery(query) AS normalized_query,
    {% else -%}
    query,
    {% endif -%}
    concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) AS read,
    concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) AS written,
    concat(toString(result_rows), ' rows / ', formatReadableSize(result_bytes)) AS result,
    formatReadableSize(memory_usage) AS "memory usage",
    exception,
    '\n' || stack_trace AS stack_trace,
    user,
    initial_user,
    multiIf(empty(client_name), http_user_agent, concat(client_name, ' ', toString(client_version_major), '.', toString(client_version_minor), '.', toString(client_version_patch))) AS client,
    client_hostname,
    {% if version_ge('21.3') -%}
    databases,
    tables,
    columns,
    used_aggregate_functions,
    used_aggregate_function_combinators,
    used_database_engines,
    used_data_type_families,
    used_dictionaries,
    used_formats,
    used_functions,
    used_storages,
    used_table_functions,
    thread_ids,
    {% endif -%}
    {% if version_ge('21.8') -%}
    ProfileEvents,
    Settings
    {% else -%}
    ProfileEvents.Names,
    ProfileEvents.Values,
    Settings.Names,
    Settings.Values
    {% endif -%}
FROM system.query_log
WHERE type != 'QueryStart'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL 1 DAY
ORDER BY query_duration_ms DESC
LIMIT 10
"""
)

SELECT_TOP_QUERIES_BY_MEMORY_USAGE = str.strip(
    # language=clickhouse
    r"""
SELECT
    type,
    query_start_time,
    query_duration_ms,
    query_id,
    {% if version_ge('21.3') -%}
    query_kind,
    {% endif -%}
    is_initial_query,
    {% if normalize_queries -%}
    normalizeQuery(query) AS normalized_query,
    {% else -%}
    query,
    {% endif -%}
    concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) AS read,
    concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) AS written,
    concat(toString(result_rows), ' rows / ', formatReadableSize(result_bytes)) AS result,
    formatReadableSize(memory_usage) AS "memory usage",
    exception,
    '\n' || stack_trace AS stack_trace,
    user,
    initial_user,
    multiIf(empty(client_name), http_user_agent, concat(client_name, ' ', toString(client_version_major), '.', toString(client_version_minor), '.', toString(client_version_patch))) AS client,
    client_hostname,
    {% if version_ge('21.3') -%}
    databases,
    tables,
    columns,
    used_aggregate_functions,
    used_aggregate_function_combinators,
    used_database_engines,
    used_data_type_families,
    used_dictionaries,
    used_formats,
    used_functions,
    used_storages,
    used_table_functions,
    thread_ids,
    {% endif -%}
    {% if version_ge('21.8') -%}
    ProfileEvents,
    Settings
    {% else -%}
    ProfileEvents.Names,
    ProfileEvents.Values,
    Settings.Names,
    Settings.Values
    {% endif -%}
FROM system.query_log
WHERE type != 'QueryStart'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL 1 DAY
ORDER BY memory_usage DESC
LIMIT 10
"""
)

SELECT_FAILED_QUERIES = str.strip(
    # language=clickhouse
    r"""
SELECT
    type,
    query_start_time,
    query_duration_ms,
    query_id,
    {% if version_ge('21.3') -%}
    query_kind,
    {% endif -%}
    is_initial_query,
    {% if normalize_queries -%}
    normalizeQuery(query) AS normalized_query,
    {% else -%}
    query,
    {% endif -%}
    concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) AS read,
    concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) AS written,
    concat(toString(result_rows), ' rows / ', formatReadableSize(result_bytes)) AS result,
    formatReadableSize(memory_usage) AS "memory usage",
    exception,
    '\n' || stack_trace AS stack_trace,
    user,
    initial_user,
    multiIf(empty(client_name), http_user_agent, concat(client_name, ' ', toString(client_version_major), '.', toString(client_version_minor), '.', toString(client_version_patch))) AS client,
    client_hostname,
    {% if version_ge('21.3') -%}
    databases,
    tables,
    columns,
    used_aggregate_functions,
    used_aggregate_function_combinators,
    used_database_engines,
    used_data_type_families,
    used_dictionaries,
    used_formats,
    used_functions,
    used_storages,
    used_table_functions,
    thread_ids,
    {% endif -%}
    {% if version_ge('21.8') -%}
    ProfileEvents,
    Settings
    {% else -%}
    ProfileEvents.Names,
    ProfileEvents.Values,
    Settings.Names,
    Settings.Values
    {% endif -%}
FROM system.query_log
WHERE type != 'QueryStart'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL 1 DAY
  AND exception != ''
ORDER BY query_start_time DESC
LIMIT 10
"""
)

SELECT_STACK_TRACES = str.strip(
    # language=clickhouse
    r"""
SELECT
    {% if version_ge('21.8') -%}
    thread_name,
    {% endif -%}
    min(thread_id) AS min_thread_id,
    count() AS threads,
    '\n' || arrayStringConcat(
       arrayMap(
           x,
           y -> concat(x, ': ', y),
           arrayMap(x -> addressToLine(x), trace),
           arrayMap(x -> demangle(addressToSymbol(x)), trace)),
       '\n') AS trace
FROM system.stack_trace
GROUP BY
    {% if version_ge('21.8') -%}
    thread_name,
    {% endif -%}
    trace
ORDER BY min_thread_id
"""
)

SELECT_CRASH_LOG = str.strip(
    # language=clickhouse
    r"""
SELECT
    event_time,
    signal,
    thread_id,
    query_id,
    '\n' || arrayStringConcat(trace_full, '\n') AS trace,
    version
FROM system.crash_log
ORDER BY event_time DESC
"""
)
