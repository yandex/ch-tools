import json
import sys
import subprocess
from datetime import datetime

from requests.exceptions import RequestException

from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseClient, ClickhouseConfig
from cloud.mdb.clickhouse.tools.common.dbaas import DbaasConfig
from humanfriendly import format_size

SELECT_VERSION = 'SELECT version()'

SELECT_UPTIME = r'''SELECT formatReadableTimeDelta(uptime())'''

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
ORDER BY create_time DESC
LIMIT 10
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
LIMIT 10
'''

SELECT_PROCESSES = r'''SELECT
    elapsed,
    query_id,
    query,
    is_cancelled,
    concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) "read",
    concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) "written",
    formatReadableSize(memory_usage) "memory usage",
    user,
    multiIf(empty(client_name), http_user_agent, concat(client_name, ' ', toString(client_version_major), '.', toString(client_version_minor), '.', toString(client_version_patch))) "client"
FROM system.processes
ORDER BY elapsed DESC
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


class DiagnosticsData:
    def __init__(self):
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

    def dump(self, stream):
        json.dump(self._sections, stream, indent=2, ensure_ascii=False)

    def _section(self, name=None):
        if self._sections[-1]['section'] != name:
            self._sections.append({'section': name, 'data': {}})

        return self._sections[-1]['data']


def main():
    timestamp = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
    client = ClickhouseClient(user='mdb_admin')
    dbaas_config = DbaasConfig.load()
    ch_config = ClickhouseConfig.load()
    version = client.query(SELECT_VERSION)

    data = DiagnosticsData()
    data.add_string('Cluster ID', dbaas_config.cluster_id)
    data.add_string('Cluster name', dbaas_config.cluster_name)
    data.add_string('Version', version)
    data.add_string('Host', dbaas_config.fqdn)
    data.add_string('Replicas', ','.join(dbaas_config.replicas))
    data.add_string('Shard count', dbaas_config.shard_count)
    data.add_string('Host count', dbaas_config.host_count)
    data.add_string('Virtualization', dbaas_config.vtype)
    data.add_string('Resource preset', format_resource_preset(dbaas_config))
    data.add_string('Storage', format_storage(dbaas_config, ch_config))
    data.add_string('Timestamp', timestamp)
    if version_ge(version, '21.3'):
        uptime = client.query(SELECT_UPTIME)
    else:
        uptime = client.query(SELECT_UPTIME_DEPRECATED)
    data.add_string('Uptime', uptime)

    add_chart_urls(data, dbaas_config)

    data.add_query(
        name='Database engines',
        query=SELECT_DATABASE_ENGINES,
        result=execute_query(client, SELECT_DATABASE_ENGINES, format='PrettyCompactNoEscapes'),
        section='Schema')
    data.add_query(
        name='Databases (top 10 by size)',
        query=SELECT_DATABASES,
        result=execute_query(client, SELECT_DATABASES, format='PrettyCompactNoEscapes'),
        section='Schema')
    data.add_query(
        name='Table engines',
        query=SELECT_DATABASE_ENGINES,
        result=execute_query(client, SELECT_DATABASE_ENGINES, format='PrettyCompactNoEscapes'),
        section='Schema')
    data.add_query(
        name='Dictionaries',
        query=SELECT_DICTIONARIES,
        result=execute_query(client, SELECT_DICTIONARIES, format='PrettyCompactNoEscapes'),
        section='Schema')

    data.add_query(
        name='Replicated tables (top 10 by absolute delay)',
        query=SELECT_REPLICAS,
        result=execute_query(client, SELECT_REPLICAS, format='PrettyCompactNoEscapes'),
        section='Replication')
    data.add_query(
        name='Replication queue (top 10 oldest tasks)',
        query=SELECT_REPLICATION_QUEUE,
        result=execute_query(client, SELECT_REPLICATION_QUEUE, format='Vertical'),
        section='Replication')

    data.add_query(
        name='Top 10 tables by max parts per partition',
        query=SELECT_PARTS_PER_TABLE,
        result=execute_query(client, SELECT_PARTS_PER_TABLE, format='PrettyCompactNoEscapes'),
        section='Merges and mutations')
    if version_ge(version, '20.3'):
        data.add_query(
            name='Merges in progress',
            query=SELECT_MERGES,
            result=execute_query(client, SELECT_MERGES, format='Vertical'),
            section='Merges and mutations')
        data.add_query(
            name='Mutations in progress',
            query=SELECT_MUTATIONS,
            result=execute_query(client, SELECT_MUTATIONS, format='Vertical'),
            section='Merges and mutations')
    else:
        data.add_query(
            name='Merges in progress',
            query=SELECT_MERGES_DEPRECATED,
            result=execute_query(client, SELECT_MERGES_DEPRECATED, format='Vertical'),
            section='Merges and mutations')
        data.add_query(
            name='Mutations in progress',
            query=SELECT_MUTATIONS_DEPRECATED,
            result=execute_query(client, SELECT_MUTATIONS_DEPRECATED, format='Vertical'),
            section='Merges and mutations')

    data.add_query(
        name='Process list',
        query=SELECT_PROCESSES,
        result=execute_query(client, SELECT_PROCESSES, format='Vertical'))

    data.add_query(
        name='Stack traces',
        query=SELECT_STACK_TRACES,
        result=execute_query(client, SELECT_STACK_TRACES, format='Vertical'))

    lsof_command = 'lsof -p $(pidof clickhouse-server)'
    data.add_command(
        name='lsof',
        command=lsof_command,
        result=execute_command(lsof_command))

    data.dump(sys.stdout)


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


def add_chart_urls(data, dbaas_config):
    cluster_id = dbaas_config.cluster_id
    host = dbaas_config.fqdn
    if dbaas_config.vtype == 'porto':
        solomon_url = 'https://solomon.yandex-team.ru'
        cluster_dashboard = f'{solomon_url}/?project=internal-mdb&service=mdb&dashboard=mdb-prod-cluster-clickhouse&cid={cluster_id}'
        host_dashboard = f'{solomon_url}/?project=internal-mdb&cluster=internal-mdb_dom0&service=dom0&host=by_cid_container&dc=by_cid_container' \
                         f'&dashboard=internal-mdb-porto-instance&l.container={host}&l.cid={cluster_id}'
    else:
        solomon_url = 'https://solomon.cloud.yandex-team.ru'
        cluster_dashboard = f'{solomon_url}/?project=yandexcloud&service=yandexcloud_dbaas&dashboard=mdb-prod-cluster-clickhouse&cid={cluster_id}'
        host_dashboard = f'{solomon_url}/?project=yandexcloud&service=yandexcloud_dbaas&dashboard=cloud-mdb-instance-system' \
                         f'&cluster=mdb_{cluster_id}&host={host}'
    data.add_url('Cluster dashboard', cluster_dashboard, section='Charts')
    data.add_url('Host dashboard', host_dashboard, section='Charts')


def execute_query(client, query, format=None):
    try:
        return client.query(query, format=format)
    except RequestException as e:
        return repr(e) if e.response is None else e.response.text


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
