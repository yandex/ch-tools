import argparse
import gzip
import json
import subprocess
import sys
import io
from datetime import datetime, timezone

import yaml
from requests.exceptions import RequestException

from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseClient, ClickhouseConfig
from cloud.mdb.clickhouse.tools.common.dbaas import DbaasConfig
from cloud.mdb.clickhouse.tools.common.utils import version_ge
from humanfriendly import format_size

SELECT_UPTIME = r'''
{% if version_ge('21.3') -%}
SELECT formatReadableTimeDelta(uptime())
{% else -%}
SELECT
    toString(floor(uptime() / 3600 / 24)) || ' days ' ||
    toString(floor(uptime() % (24 * 3600) / 3600, 1)) || ' hours'
{% endif -%}
'''

SELECT_SYSTEM_TABLES = "SELECT name FROM system.tables WHERE database = 'system'"

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
{% if version_ge('20.3') -%}
    result_part_path,
    source_part_paths,
{% endif -%}
    num_parts,
    formatReadableSize(total_size_bytes_compressed) "total_size_compressed",
    formatReadableSize(bytes_read_uncompressed) "read_uncompressed",
    formatReadableSize(bytes_written_uncompressed) "written_uncompressed",
    columns_written,
{% if version_ge('20.3') -%}
    formatReadableSize(memory_usage) "memory_usage",
    thread_id
{% else -%}
    formatReadableSize(memory_usage) "memory_usage"
{% endif -%}
FROM system.merges
'''

SELECT_MUTATIONS = r'''SELECT
    database,
    table,
    mutation_id,
    command,
    create_time,
{% if version_ge('20.3') -%}
    parts_to_do_names,
{% endif -%}
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
{% if version_ge('20.3') -%}
    part_type,
{% endif -%}
    active,
    level,
{% if version_ge('20.3') -%}
    disk_name,
{% endif -%}
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
'''

SELECT_STACK_TRACES = r'''SELECT
    thread_name,
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
GROUP BY thread_name, trace
ORDER BY min_thread_id
'''

SELECT_CRASH_LOG = r'''SELECT
    event_time,
    signal,
    thread_id,
    query_id,
    '\n' || arrayStringConcat(trace_full, '\n') AS trace,
    version
FROM system.crash_log
ORDER BY event_time DESC
'''


class DiagnosticsData:
    def __init__(self, args, host):
        self.args = args
        self.host = host
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
            result = self._dump_json()
        elif format.startswith('yaml'):
            result = self._dump_yaml()
        else:
            result = self._dump_wiki()

        if format.endswith('.gz'):
            compressor = gzip.GzipFile(mode='wb', fileobj=sys.stdout.buffer)
            compressor.write(result.encode())
        else:
            print(result)

    def _section(self, name=None):
        if self._sections[-1]['section'] != name:
            self._sections.append({'section': name, 'data': {}})

        return self._sections[-1]['data']

    def _dump_json(self):
        """
        Dump diagnostic data in JSON format.
        """
        return json.dumps(self._sections, indent=2, ensure_ascii=False)

    def _dump_yaml(self):
        """
        Dump diagnostic data in YAML format.
        """
        return yaml.dump(self._sections, default_flow_style=False, allow_unicode=True)

    def _dump_wiki(self):
        """
        Dump diagnostic data in Yandex wiki format.
        """
        def _write_title(buffer, value):
            buffer.write(f'===+ {value}\n')

        def _write_subtitle(buffer, value):
            buffer.write(f'====+ {value}\n')

        def _write_string_item(buffer, name, item):
            value = item['value']
            if value != '':
                value = f'**{value}**'
            buffer.write(f'{name}: {value}\n')

        def _write_url_item(buffer, name, item):
            value = item['value']
            buffer.write(f'**{name}**\n{value}\n')

        def _write_xml_item(buffer, section_name, name, item):
            if section_name:
                buffer.write(f'=====+ {name}\n')
            else:
                _write_subtitle(buffer, name)

            _write_result(buffer, item['value'], format='XML')

        def _write_query_item(buffer, section_name, name, item):
            if section_name:
                buffer.write(f'=====+ {name}\n')
            else:
                _write_subtitle(buffer, name)

            _write_query(buffer, item['query'])
            _write_result(buffer, item['result'])

        def _write_command_item(buffer, section_name, name, item):
            if section_name:
                buffer.write(f'=====+ {name}\n')
            else:
                _write_subtitle(buffer, name)

            _write_command(buffer, item['command'])
            _write_result(buffer, item['result'])

        def _write_unknown_item(buffer, section_name, name, item):
            if section_name:
                buffer.write(f'**{name}**\n')
            else:
                _write_subtitle(buffer, name)

            json.dump(item, buffer, indent=2)

        def _write_query(buffer, query):
            buffer.write('<{ query\n')
            buffer.write('%%(SQL)\n')
            buffer.write(query)
            buffer.write('\n%%\n')
            buffer.write('}>\n\n')

        def _write_command(buffer, command):
            buffer.write('<{ command\n')
            buffer.write('%%\n')
            buffer.write(command)
            buffer.write('\n%%\n')
            buffer.write('}>\n\n')

        def _write_result(buffer, result, format=None):
            buffer.write(f'%%({format})\n' if format else '%%\n')
            buffer.write(result)
            buffer.write('\n%%\n')

        buffer = io.StringIO()

        _write_title(buffer, f'Diagnostics data for host {self.host}')
        for section in self._sections:
            section_name = section['section']
            if section_name:
                _write_subtitle(buffer, section_name)

            for name, item in section['data'].items():
                if item['type'] == 'string':
                    _write_string_item(buffer, name, item)
                elif item['type'] == 'url':
                    _write_url_item(buffer, name, item)
                elif item['type'] == 'query':
                    _write_query_item(buffer, section_name, name, item)
                elif item['type'] == 'command':
                    _write_command_item(buffer, section_name, name, item)
                elif item['type'] == 'xml':
                    _write_xml_item(buffer, section_name, name, item)
                else:
                    _write_unknown_item(buffer, section_name, name, item)

        return buffer.getvalue()


def main():
    args = parse_args()

    timestamp = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
    client = ClickhouseClient()
    dbaas_config = DbaasConfig.load()
    ch_config = ClickhouseConfig.load()
    version = client.clickhouse_version
    system_tables = [row[0] for row in execute_query(client, SELECT_SYSTEM_TABLES, format='JSONCompact')['data']]

    diagnostics = DiagnosticsData(args, dbaas_config.fqdn)
    diagnostics.add_string('Cluster ID', dbaas_config.cluster_id)
    diagnostics.add_string('Cluster name', dbaas_config.cluster_name)
    diagnostics.add_string('Version', version)
    diagnostics.add_string('Host', dbaas_config.fqdn)
    diagnostics.add_string('Replicas', ','.join(dbaas_config.replicas))
    diagnostics.add_string('Shard count', dbaas_config.shard_count)
    diagnostics.add_string('Host count', dbaas_config.host_count)
    diagnostics.add_string('Virtualization', dbaas_config.vtype)
    diagnostics.add_string('Resource preset', format_resource_preset(dbaas_config))
    if args.full:
        diagnostics.add_string('Storage', format_storage(dbaas_config, ch_config))
    diagnostics.add_string('Timestamp', timestamp)
    diagnostics.add_string('Uptime', execute_query(client, SELECT_UPTIME))

    if args.full:
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
              query=SELECT_TABLE_ENGINES,
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
    add_query(diagnostics, 'Merges in progress',
              client=client,
              query=SELECT_MERGES,
              format='Vertical')
    add_query(diagnostics, 'Mutations in progress',
              client=client,
              query=SELECT_MUTATIONS,
              format='Vertical')
    add_query(diagnostics, 'Recent data parts (modification time within last 3 minutes)',
              client=client,
              query=SELECT_RECENT_DATA_PARTS,
              format='Vertical')

    add_query(diagnostics, 'system.detached_parts',
              client=client,
              query=SELECT_DETACHED_DATA_PARTS,
              format='PrettyCompactNoEscapes',
              section='Detached data')
    add_command(diagnostics, 'Disk space usage',
                command='du -sh -L -c /var/lib/clickhouse/data/*/*/detached/* | sort -rsh',
                section='Detached data')

    add_query(diagnostics, 'Queries in progress (process list)',
              client=client,
              query=SELECT_PROCESSES,
              format='Vertical',
              section='Queries')
    add_query(diagnostics, 'Top 10 queries by duration',
              client=client,
              query=SELECT_TOP_QUERIES_BY_DURATION,
              format='Vertical',
              section='Queries')
    add_query(diagnostics, 'Top 10 queries by memory usage',
              client=client,
              query=SELECT_TOP_QUERIES_BY_MEMORY_USAGE,
              format='Vertical',
              section='Queries')
    add_query(diagnostics, 'Last 10 failed queries',
              client=client,
              query=SELECT_FAILED_QUERIES,
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

    if args.full:
        add_command(diagnostics, 'monrun', r'monrun | sed -r "s/\x1B\[([0-9]{1,3}(;[0-9]{1,2})?)?[mGK]//g"')

    add_command(diagnostics, 'lsof', 'lsof -p $(pidof clickhouse-server)')

    diagnostics.dump(args.format)


def parse_args():
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--format',
                        choices=['json', 'yaml', 'json.gz', 'yaml.gz', 'wiki', 'wiki.gz'],
                        default='wiki')
    parser.add_argument('--normalize-queries',
                        action='store_true',
                        default=False)
    parser.add_argument('--full',
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
        result=execute_query(client, query, render_query=False, format=format),
        section=section)


def execute_query(client, query, render_query=True, format=None):
    if render_query:
        query = client.render_query(query)

    try:
        return client.query(query, format=format)
    except RequestException as e:
        return repr(e) if e.response is None else e.response.text


def add_command(diagnostics, name, command, section=None):
    diagnostics.add_command(
        name=name,
        command=command,
        result=execute_command(command),
        section=section)


def execute_command(command, input=None):
    proc = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if isinstance(input, str):
        input = input.encode()

    stdout, stderr = proc.communicate(input=input)

    if proc.returncode:
        return f'failed with exit code {proc.returncode}\n{stderr.decode()}'

    return stdout.decode()


if __name__ == '__main__':
    main()
