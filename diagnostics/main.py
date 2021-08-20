import argparse
import gzip
import json
import subprocess
import sys
from datetime import datetime, timezone

import yaml
from requests.exceptions import RequestException

from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseClient, ClickhouseConfig
from cloud.mdb.clickhouse.tools.common.dbaas import DbaasConfig
from humanfriendly import format_size

SELECT_VERSION = 'SELECT version()'

SELECT_UPTIME = r'''SELECT formatReadableTimeDelta(uptime())'''

SELECT_SYSTEM_TABLES = "SELECT name FROM system.tables WHERE database = 'system'"

SELECT_UPTIME_DEPRECATED = r'''SELECT
    toString(floor(uptime()/3600/24)) || ' days ' ||
    toString(floor(uptime() % (24 * 3600) / 3600, 1)) || ' hours'
'''

SELECT_DATABASE_ENGINES = r'''SELECT
    engine,
    count() "count"
FROM system.databases
GROUP BY engine
'''

SELECT_DATABASES = r'''SELECT
    name,
    engine,
    tables,
    partitions,
    parts,
    formatReadableSize(bytes_on_disk) "disk_size"
FROM system.databases db
LEFT JOIN
(
    SELECT
        database,
        uniq(table) "tables",
        uniq(table, partition) "partitions",
        count() AS parts,
        sum(bytes_on_disk) "bytes_on_disk"
    FROM system.parts
    WHERE active
    GROUP BY database
) AS db_stats ON db.name = db_stats.database
ORDER BY bytes_on_disk DESC
LIMIT 10
'''

SELECT_TABLE_ENGINES = r'''SELECT
    engine,
    count() "count"
FROM system.tables
WHERE database != 'system'
GROUP BY engine
'''

SELECT_DICTIONARIES = r'''SELECT
    source,
    type,
    status,
    count() "count"
FROM system.dictionaries
GROUP BY source, type, status
ORDER BY status DESC, source
'''

SELECT_ACCESS = "SHOW ACCESS"

SELECT_QUOTA_USAGE = "SHOW QUOTA"

SELECT_REPLICAS = r'''SELECT
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
'''

SELECT_REPLICATION_QUEUE = r'''SELECT
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
    concat('time: ', toString(last_postpone_time), ', number: ', toString(num_postponed), ', reason: ', postpone_reason) postpone
FROM system.replication_queue
ORDER BY create_time ASC
LIMIT 20
'''

SELECT_REPLICATED_FETCHES = r'''SELECT
    database,
    table,
    round(elapsed, 1) "elapsed",
    round(100 * progress, 1) "progress",
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
'''

SELECT_PARTS_PER_TABLE = r'''SELECT
        database,
    table,
    count() "partitions",
    sum(part_count) "parts",
    max(part_count) "max_parts_per_partition"
FROM
(
    SELECT
        database,
        table,
        partition,
        count() "part_count"
    FROM system.parts
    WHERE active
    GROUP BY database, table, partition
) partitions
GROUP BY database, table
ORDER BY max_parts_per_partition DESC
LIMIT 10
'''

SELECT_MERGES = r'''SELECT
    database,
    table,
    round(elapsed, 1) "elapsed",
    round(100 * progress, 1) "progress",
    is_mutation,
    partition_id,
    result_part_path,
    source_part_paths,
    num_parts,
    formatReadableSize(total_size_bytes_compressed) "total_size_compressed",
    formatReadableSize(bytes_read_uncompressed) "read_uncompressed",
    formatReadableSize(bytes_written_uncompressed) "written_uncompressed",
    columns_written,
    formatReadableSize(memory_usage) "memory_usage",
    thread_id
FROM system.merges
'''

SELECT_MERGES_DEPRECATED = r'''SELECT
    database,
    table,
    round(elapsed, 1) "elapsed",
    round(100 * progress, 1) "progress",
    is_mutation,
    partition_id,
    num_parts,
    formatReadableSize(total_size_bytes_compressed) "total_size_compressed",
    formatReadableSize(bytes_read_uncompressed) "read_uncompressed",
    formatReadableSize(bytes_written_uncompressed) "written_uncompressed",
    columns_written,
    formatReadableSize(memory_usage) "memory_usage"
FROM system.merges
'''

SELECT_MUTATIONS = r'''SELECT
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
'''

SELECT_MUTATIONS_DEPRECATED = r'''SELECT
    database,
    table,
    mutation_id,
    command,
    create_time,
    parts_to_do,
    is_done,
    latest_failed_part,
    latest_fail_time,
    latest_fail_reason
FROM system.mutations
WHERE NOT is_done
ORDER BY create_time DESC
'''

SELECT_RECENT_DATA_PARTS = r'''SELECT
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
'''

SELECT_RECENT_DATA_PARTS_DEPRECATED = r'''SELECT
    database,
    table,
    engine,
    partition_id,
    name,
    active,
    level,
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
'''

SELECT_DETACHED_DATA_PARTS = r'''SELECT
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
'''

SELECT_PROCESSES = r'''SELECT
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
    thread_ids,
    ProfileEvents.Names,
    ProfileEvents.Values,
    Settings.Names,
    Settings.Values
FROM system.processes
ORDER BY elapsed DESC
'''

SELECT_PROCESSES_DEPRECATED = r'''SELECT
    elapsed,
    query_id,
    query,
    is_cancelled,
    concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) AS read,
    concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) AS written,
    formatReadableSize(memory_usage) AS "memory usage",
    user,
    multiIf(empty(client_name), http_user_agent, concat(client_name, ' ', toString(client_version_major), '.', toString(client_version_minor), '.', toString(client_version_patch))) AS client,
    ProfileEvents.Names,
    ProfileEvents.Values,
    Settings.Names,
    Settings.Values
FROM system.processes
ORDER BY elapsed DESC
'''

SELECT_TOP_QUERIES_BY_DURATION = r'''SELECT
    type,
    query_start_time,
    query_duration_ms,
    query_id,
    query_kind,
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
    ProfileEvents.Names,
    ProfileEvents.Values,
    Settings.Names,
    Settings.Values
FROM system.query_log
WHERE type != 'QueryStart'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL 1 DAY
ORDER BY query_duration_ms DESC
LIMIT 10
'''

SELECT_TOP_QUERIES_BY_DURATION_DEPRECATED = r'''SELECT
    type,
    query_start_time,
    query_duration_ms,
    query_id,
    is_initial_query,
    query,
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
    ProfileEvents.Names,
    ProfileEvents.Values,
    Settings.Names,
    Settings.Values
FROM system.query_log
WHERE type != 'QueryStart'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL 1 DAY
ORDER BY query_duration_ms DESC
LIMIT 10
'''

SELECT_TOP_QUERIES_BY_MEMORY_USAGE = r'''SELECT
    type,
    query_start_time,
    query_duration_ms,
    query_id,
    query_kind,
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
    ProfileEvents.Names,
    ProfileEvents.Values,
    Settings.Names,
    Settings.Values
FROM system.query_log
WHERE type != 'QueryStart'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL 1 DAY
ORDER BY memory_usage DESC
LIMIT 10
'''

SELECT_TOP_QUERIES_BY_MEMORY_USAGE_DEPRECATED = r'''SELECT
    type,
    query_start_time,
    query_duration_ms,
    query_id,
    is_initial_query,
    query,
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
    ProfileEvents.Names,
    ProfileEvents.Values,
    Settings.Names,
    Settings.Values
FROM system.query_log
WHERE type != 'QueryStart'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL 1 DAY
ORDER BY memory_usage DESC
LIMIT 10
'''

SELECT_FAILED_QUERIES = r'''SELECT
    type,
    query_start_time,
    query_duration_ms,
    query_id,
    query_kind,
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
    ProfileEvents.Names,
    ProfileEvents.Values,
    Settings.Names,
    Settings.Values
FROM system.query_log
WHERE type != 'QueryStart'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL 1 DAY
  AND exception != ''
ORDER BY query_start_time DESC
LIMIT 10
'''

SELECT_FAILED_QUERIES_DEPRECATED = r'''SELECT
    type,
    query_start_time,
    query_duration_ms,
    query_id,
    is_initial_query,
    query,
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
    ProfileEvents.Names,
    ProfileEvents.Values,
    Settings.Names,
    Settings.Values
FROM system.query_log
WHERE type != 'QueryStart'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL 1 DAY
  AND exception != ''
ORDER BY query_start_time DESC
LIMIT 10
'''

SELECT_STACK_TRACES = r'''SELECT
    '\n' || arrayStringConcat(
       arrayMap(
           x,
           y -> concat(x, ': ', y),
           arrayMap(x -> addressToLine(x), trace),
           arrayMap(x -> demangle(addressToSymbol(x)), trace)),
       '\n') AS trace
FROM system.stack_trace
'''

SELECT_CRASH_LOG = r'''SELECT
    event_time,
    signal,
    thread_id,
    query_id,
    '\n' || arrayStringConcat(trace_full, '\n') AS trace,
    version
FROM system.crash_log
'''


class DiagnosticsData:
    def __init__(self, args):
        self.args = args
        self._sections = [{'section': None, 'data': {}}]

    def add_string(self, name, value, section=None):
        self._section(section)[name] = {
            'type': 'string',
            'value': value,
        }

    def add_url(self, name, value, section=None):
        self._section(section)[name] = {
            'type': 'url',
            'value': value,
        }

    def add_xml_document(self, name, document, section=None):
        self._section(section)[name] = {
            'type': 'xml',
            'value': document,
        }

    def add_query(self, name, query, result, section=None):
        self._section(section)[name] = {
            'type': 'query',
            'query': query,
            'result': result,
        }

    def add_command(self, name, command, result, section=None):
        self._section(section)[name] = {
            'type': 'command',
            'command': command,
            'result': result,
        }

    def dump(self, format):
        if format.startswith('json'):
            result = json.dumps(self._sections, indent=2, ensure_ascii=False)
        else:
            result = yaml.dump(self._sections, default_flow_style=False, allow_unicode=True)

        if format.endswith('.gz'):
            compressor = gzip.GzipFile(mode='wb', fileobj=sys.stdout.buffer)
            compressor.write(result.encode())
        else:
            print(result)

    def _section(self, name=None):
        if self._sections[-1]['section'] != name:
            self._sections.append({'section': name, 'data': {}})

        return self._sections[-1]['data']


def main():
    args = parse_args()

    timestamp = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
    client = ClickhouseClient()
    dbaas_config = DbaasConfig.load()
    ch_config = ClickhouseConfig.load()
    version = client.query(SELECT_VERSION)
    system_tables = [row[0] for row in client.query(SELECT_SYSTEM_TABLES, format='JSONCompact')['data']]

    diagnostics = DiagnosticsData(args)
    diagnostics.add_string('Cluster ID', dbaas_config.cluster_id)
    diagnostics.add_string('Cluster name', dbaas_config.cluster_name)
    diagnostics.add_string('Version', version)
    diagnostics.add_string('Host', dbaas_config.fqdn)
    diagnostics.add_string('Replicas', ','.join(dbaas_config.replicas))
    diagnostics.add_string('Shard count', dbaas_config.shard_count)
    diagnostics.add_string('Host count', dbaas_config.host_count)
    diagnostics.add_string('Virtualization', dbaas_config.vtype)
    diagnostics.add_string('Resource preset', format_resource_preset(dbaas_config))
    diagnostics.add_string('Storage', format_storage(dbaas_config, ch_config))
    diagnostics.add_string('Timestamp', timestamp)
    if version_ge(version, '21.3'):
        uptime = client.query(SELECT_UPTIME)
    else:
        uptime = client.query(SELECT_UPTIME_DEPRECATED)
    diagnostics.add_string('Uptime', uptime)

    add_chart_urls(diagnostics, dbaas_config)

    diagnostics.add_xml_document('ClickHouse configuration', ch_config.dump())

    if version_ge(version, '20.8'):
        add_query(diagnostics, 'Access configuration',
                  client=client,
                  query=SELECT_ACCESS,
                  format='TSVRaw')
        add_query(diagnostics, 'Quotas',
                  client=client,
                  query=SELECT_QUOTA_USAGE,
                  format='Vertical')

    add_query(diagnostics, 'Database engines',
              client=client,
              query=SELECT_DATABASE_ENGINES,
              format='PrettyCompactNoEscapes',
              section='Schema')
    add_query(diagnostics, 'Databases (top 10 by size)',
              client=client,
              query=SELECT_DATABASES,
              format='PrettyCompactNoEscapes',
              section='Schema')
    add_query(diagnostics, 'Table engines',
              client=client,
              query=SELECT_DATABASE_ENGINES,
              format='PrettyCompactNoEscapes',
              section='Schema')
    add_query(diagnostics, 'Dictionaries',
              client=client,
              query=SELECT_DICTIONARIES,
              format='PrettyCompactNoEscapes',
              section='Schema')

    add_query(diagnostics, 'Replicated tables (top 10 by absolute delay)',
              client=client,
              query=SELECT_REPLICAS,
              format='PrettyCompactNoEscapes',
              section='Replication')
    add_query(diagnostics, 'Replication queue (top 20 oldest tasks)',
              client=client,
              query=SELECT_REPLICATION_QUEUE,
              format='Vertical',
              section='Replication')
    if version_ge(version, '21.3'):
        add_query(diagnostics, 'Replicated fetches',
                  client=client,
                  query=SELECT_REPLICATED_FETCHES,
                  format='Vertical',
                  section='Replication')

    add_query(diagnostics, 'Top 10 tables by max parts per partition',
              client=client,
              query=SELECT_PARTS_PER_TABLE,
              format='PrettyCompactNoEscapes')
    if version_ge(version, '20.3'):
        select_merges = SELECT_MERGES
        select_mutations = SELECT_MUTATIONS
        select_recent_data_parts = SELECT_RECENT_DATA_PARTS
    else:
        select_merges = SELECT_MERGES_DEPRECATED
        select_mutations = SELECT_MUTATIONS_DEPRECATED
        select_recent_data_parts = SELECT_RECENT_DATA_PARTS_DEPRECATED
    add_query(diagnostics, 'Merges in progress',
              client=client,
              query=select_merges,
              format='Vertical')
    add_query(diagnostics, 'Mutations in progress',
              client=client,
              query=select_mutations,
              format='Vertical')
    add_query(diagnostics, 'Recent data parts (modification time within last 3 minutes)',
              client=client,
              query=select_recent_data_parts,
              format='Vertical')
    add_query(diagnostics, 'Detached data parts',
              client=client,
              query=SELECT_DETACHED_DATA_PARTS,
              format='PrettyCompactNoEscapes')

    if version_ge(version, '21.3'):
        select_processes = SELECT_PROCESSES
        select_top_queries_by_duration = SELECT_TOP_QUERIES_BY_DURATION
        select_top_queries_by_memory_usage = SELECT_TOP_QUERIES_BY_MEMORY_USAGE
        select_failed_queries = SELECT_FAILED_QUERIES
    else:
        select_processes = SELECT_PROCESSES_DEPRECATED
        select_top_queries_by_duration = SELECT_TOP_QUERIES_BY_DURATION_DEPRECATED
        select_top_queries_by_memory_usage = SELECT_TOP_QUERIES_BY_MEMORY_USAGE_DEPRECATED
        select_failed_queries = SELECT_FAILED_QUERIES_DEPRECATED
    add_query(diagnostics, 'Queries in progress (process list)',
              client=client,
              query=select_processes,
              format='Vertical',
              section='Queries')
    add_query(diagnostics, 'Top 10 queries by duration',
              client=client,
              query=select_top_queries_by_duration,
              format='Vertical',
              section='Queries')
    add_query(diagnostics, 'Top 10 queries by memory usage',
              client=client,
              query=select_top_queries_by_memory_usage,
              format='Vertical',
              section='Queries')
    add_query(diagnostics, 'Last 10 failed queries',
              client=client,
              query=select_failed_queries,
              format='Vertical',
              section='Queries')

    add_query(diagnostics, 'Stack traces',
              client=client,
              query=SELECT_STACK_TRACES,
              format='Vertical')

    if 'crash_log' in system_tables:
        add_query(diagnostics, 'Crash log',
                  client=client,
                  query=SELECT_CRASH_LOG,
                  format='Vertical')

    add_command(diagnostics, 'monrun', r'monrun | sed -r "s/\x1B\[([0-9]{1,3}(;[0-9]{1,2})?)?[mGK]//g"')

    add_command(diagnostics, 'lsof', 'lsof -p $(pidof clickhouse-server)')

    diagnostics.dump(args.format)


def parse_args():
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--format',
                        choices=['json', 'yaml', 'json.gz', 'yaml.gz'],
                        default='json')
    parser.add_argument('--normalize-queries',
                        action='store_true',
                        default=False)
    return parser.parse_args()


def format_resource_preset(dbaas_config):
    name = dbaas_config.flavor

    if dbaas_config.cpu_fraction < 100:
        cpu = f'{int(dbaas_config.cpu_limit)} * {dbaas_config.cpu_fraction}% CPU cores'
    else:
        cpu = f'{int(dbaas_config.cpu_guarantee)} CPU cores'

    memory = f'{format_size(dbaas_config.memory_guarantee, binary=True)} memory'

    return f'{name} ({cpu} / {memory})'


def format_storage(dbaas_config, ch_config):
    disk_type = dbaas_config.disk_type
    disk_size = format_size(dbaas_config.disk_size, binary=True)

    storage = f'{disk_size} {disk_type}'
    if ch_config.has_disk('object_storage'):
        storage += ' + S3'

    return storage


def add_chart_urls(diagnostics, dbaas_config):
    cluster_id = dbaas_config.cluster_id
    host = dbaas_config.fqdn
    end_time = datetime.strftime(datetime.now(timezone.utc), '%Y-%m-%dT%H:%M:%S.000Z')
    if dbaas_config.vtype == 'porto':
        solomon_url = 'https://solomon.yandex-team.ru'
        cluster_dashboard = f'{solomon_url}/?project=internal-mdb&service=mdb&dashboard=mdb-prod-cluster-clickhouse&cid={cluster_id}&e={end_time}'
        host_dashboard = f'{solomon_url}/?project=internal-mdb&cluster=internal-mdb_dom0&service=dom0&host=by_cid_container&dc=by_cid_container' \
                         f'&dashboard=internal-mdb-porto-instance&l.container={host}&l.cid={cluster_id}&e={end_time}'
    else:
        solomon_url = 'https://solomon.cloud.yandex-team.ru'
        cluster_dashboard = f'{solomon_url}/?project=yandexcloud&service=yandexcloud_dbaas&dashboard=mdb-prod-cluster-clickhouse&cid={cluster_id}&e={end_time}'
        host_dashboard = f'{solomon_url}/?project=yandexcloud&service=yandexcloud_dbaas&dashboard=cloud-mdb-instance-system' \
                         f'&cluster=mdb_{cluster_id}&host={host}&e={end_time}'
    diagnostics.add_url('Cluster dashboard', cluster_dashboard, section='Charts')
    diagnostics.add_url('Host dashboard', host_dashboard, section='Charts')


def add_query(diagnostics, name, client, query, format, section=None):
    query_args = {
        'normalize_queries': diagnostics.args.normalize_queries,
    }
    query = client.render_query(query, **query_args)
    diagnostics.add_query(
        name=name,
        query=query,
        result=execute_query(client, query, format=format),
        section=section)


def execute_query(client, query, format=None):
    try:
        return client.query(query, format=format)
    except RequestException as e:
        return repr(e) if e.response is None else e.response.text


def add_command(diagnostics, name, command):
    diagnostics.add_command(
        name=name,
        command=command,
        result=execute_command(command))


def execute_command(command, input=None):
    proc = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if isinstance(input, str):
        input = input.encode()

    stdout, stderr = proc.communicate(input=input)

    if proc.returncode:
        return f'failed with exit code {proc.returncode}\n{stderr.decode()}'

    return stdout.decode()


def version_ge(version1, version2):
    return parse_version(version1) > parse_version(version2)


def parse_version(version):
    return [int(x) for x in version.strip().split('.')]


if __name__ == '__main__':
    main()
