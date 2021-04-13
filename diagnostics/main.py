import json
import sys
from collections import OrderedDict
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
LIMIT 10
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


def main():
    timestamp = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
    client = ClickhouseClient(user='mdb_admin')
    dbaas_config = DbaasConfig.load()
    ch_config = ClickhouseConfig.load()
    version = client.query(SELECT_VERSION)

    result = OrderedDict()
    result['host'] = dbaas_config.fqdn
    result['version'] = version
    result['cluster_id'] = dbaas_config.cluster_id
    result['cluster_name'] = dbaas_config.cluster_name
    result['shard_count'] = dbaas_config.shard_count
    result['host_count'] = dbaas_config.host_count
    result['virtualization'] = dbaas_config.vtype
    result['cluster_name'] = dbaas_config.cluster_name
    result['resource_preset'] = format_resource_preset(dbaas_config)
    result['storage'] = format_storage(dbaas_config, ch_config)
    result['timestamp'] = timestamp
    if version_ge(version, '21.3'):
        result['uptime'] = client.query(SELECT_UPTIME)
    else:
        result['uptime'] = client.query(SELECT_UPTIME_DEPRECATED)
    result['charts_url'] = format_charts_url(dbaas_config)
    result['database_engines'] = execute(client, SELECT_DATABASE_ENGINES, format='PrettyCompactNoEscapes')
    result['databases'] = execute(client, SELECT_DATABASES, format='PrettyCompactNoEscapes')
    result['table_engines'] = execute(client, SELECT_TABLE_ENGINES, format='PrettyCompactNoEscapes')
    result['dictionaries'] = execute(client, SELECT_DICTIONARIES, format='PrettyCompactNoEscapes')
    result['replicas'] = dbaas_config.shard_hosts
    result['replicated_tables'] = execute(client, SELECT_REPLICAS, format='PrettyCompactNoEscapes')
    result['replication_queue'] = execute(client, SELECT_REPLICATION_QUEUE, format='Vertical')
    result['parts_per_table'] = execute(client, SELECT_PARTS_PER_TABLE, format='PrettyCompactNoEscapes')
    if version_ge(version, '20.3'):
        result['merges'] = execute(client, SELECT_MERGES, format='Vertical')
        result['mutations'] = execute(client, SELECT_MUTATIONS, format='Vertical')
    else:
        result['merges'] = execute(client, SELECT_MERGES_DEPRECATED, format='Vertical')
        result['mutations'] = execute(client, SELECT_MUTATIONS_DEPRECATED, format='Vertical')
    result['processes'] = execute(client, SELECT_PROCESSES, format='Vertical')
    result['stack_traces'] = execute(client, SELECT_STACK_TRACES, format='Vertical')

    json.dump(result, sys.stdout, indent=2, ensure_ascii=False)


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


def format_charts_url(dbaas_config):
    cluster_id = dbaas_config.cluster_id
    if dbaas_config.vtype == 'porto':
        return f'https://solomon.yandex-team.ru/?project=internal-mdb&service=mdb&dashboard=mdb-prod-cluster-clickhouse&cid={cluster_id}'
    else:
        return f'https://solomon.cloud.yandex-team.ru/?project=yandexcloud&service=yandexcloud_dbaas&dashboard=mdb-prod-cluster-clickhouse&cid={cluster_id}'


def execute(client, query, format=None):
    try:
        result = client.query(query, format=format)
    except RequestException as e:
        result = repr(e) if e.response is None else e.response.text

    return OrderedDict((
        ('query', query),
        ('result', result),
    ))


def version_ge(version1, version2):
    return parse_version(version1) > parse_version(version2)


def parse_version(version):
    return [int(x) for x in version.strip().split('.')]


if __name__ == '__main__':
    main()
