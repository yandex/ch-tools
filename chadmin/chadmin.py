#!/usr/bin/env python3
import logging

from click import Choice, group, option, pass_context
from cloud.mdb.clickhouse.tools.chadmin.cli.config_command import config_command
from cloud.mdb.internal.python.cli.parameters import TimeSpanParamType

from cloud.mdb.clickhouse.tools.chadmin.cli.chs3_backup_group import chs3_backup_group
from cloud.mdb.clickhouse.tools.chadmin.cli.crash_log_group import crash_log_group
from cloud.mdb.clickhouse.tools.chadmin.cli.data_store_group import data_store_group
from cloud.mdb.clickhouse.tools.chadmin.cli.database_group import database_group
from cloud.mdb.clickhouse.tools.chadmin.cli.dictionary_group import dictionary_group
from cloud.mdb.clickhouse.tools.chadmin.cli.disk_group import disks_group
from cloud.mdb.clickhouse.tools.chadmin.cli.list_async_metrics_command import list_async_metrics_command
from cloud.mdb.clickhouse.tools.chadmin.cli.list_events_command import list_events_command
from cloud.mdb.clickhouse.tools.chadmin.cli.list_functions_command import list_functions_command
from cloud.mdb.clickhouse.tools.chadmin.cli.list_macros_command import list_macros_command
from cloud.mdb.clickhouse.tools.chadmin.cli.list_metrics_command import list_metrics_command
from cloud.mdb.clickhouse.tools.chadmin.cli.list_settings_command import list_settings_command
from cloud.mdb.clickhouse.tools.chadmin.cli.merge_group import merge_group
from cloud.mdb.clickhouse.tools.chadmin.cli.mutation_group import mutation_group
from cloud.mdb.clickhouse.tools.chadmin.cli.object_storage_group import object_storage_group
from cloud.mdb.clickhouse.tools.chadmin.cli.part_group import part_group
from cloud.mdb.clickhouse.tools.chadmin.cli.part_log_group import part_log_group
from cloud.mdb.clickhouse.tools.chadmin.cli.partition_group import partition_group
from cloud.mdb.clickhouse.tools.chadmin.cli.process_group import process_group
from cloud.mdb.clickhouse.tools.chadmin.cli.query_log_group import query_log_group
from cloud.mdb.clickhouse.tools.chadmin.cli.replicated_fetch_group import replicated_fetch_group
from cloud.mdb.clickhouse.tools.chadmin.cli.replication_queue import replication_queue_group
from cloud.mdb.clickhouse.tools.chadmin.cli.restore_replica_command import restore_replica_command
from cloud.mdb.clickhouse.tools.chadmin.cli.stack_trace_command import stack_trace_command
from cloud.mdb.clickhouse.tools.chadmin.cli.table_group import table_group
from cloud.mdb.clickhouse.tools.chadmin.cli.table_replica_group import table_replica_group
from cloud.mdb.clickhouse.tools.chadmin.cli.thread_log_group import thread_log_group
from cloud.mdb.clickhouse.tools.chadmin.cli.wait_started_command import wait_started_command
from cloud.mdb.clickhouse.tools.chadmin.cli.zookeeper_group import zookeeper_group
from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseClient

LOG_FILE = '/var/log/chadmin/chadmin.log'


@group(context_settings=dict(help_option_names=['-h', '--help'], terminal_width=120))
@option('-f', '--format', type=Choice(['json', 'yaml', 'table']), help="Output format.")
@option(
    '--setting',
    'settings',
    multiple=True,
    type=(str, str),
    metavar='NAME VALUE',
    help="Name and value of ClickHouse setting to override."
    " Can be specified multiple times to override several settings.",
)
@option('--timeout', type=TimeSpanParamType(), help="Timeout for SQL queries.")
@option('--port', type=int, help="Port to connect.")
@option('-d', '--debug', is_flag=True, help="Enable debug output.")
@pass_context
def cli(ctx, format, settings, timeout, port, debug):
    """ClickHouse administration tool."""

    handlers = [logging.FileHandler(LOG_FILE)]
    if debug:
        handlers.append(logging.StreamHandler())

    logConfig = {
        'level': logging.DEBUG if debug else logging.INFO,
        'format': '%(asctime)s [%(levelname)s]:%(message)s',
        'handlers': handlers,
    }
    logging.basicConfig(**logConfig)

    timeout_seconds = timeout.total_seconds() if timeout else None
    settings = {item[0]: item[1] for item in settings}

    chcli = ClickhouseClient(port=port, timeout=timeout_seconds, settings=settings)

    ctx.obj = dict(chcli=chcli, format=format, debug=debug)


cli.add_command(database_group)
cli.add_command(table_group)
cli.add_command(table_replica_group)
cli.add_command(partition_group)
cli.add_command(part_group)
cli.add_command(dictionary_group)
cli.add_command(merge_group)
cli.add_command(mutation_group)
cli.add_command(query_log_group)
cli.add_command(thread_log_group)
cli.add_command(part_log_group)
cli.add_command(crash_log_group)
cli.add_command(process_group)
cli.add_command(replication_queue_group)
cli.add_command(replicated_fetch_group)
cli.add_command(restore_replica_command)
cli.add_command(list_settings_command)
cli.add_command(list_functions_command)
cli.add_command(list_macros_command)
cli.add_command(list_events_command)
cli.add_command(list_metrics_command)
cli.add_command(list_async_metrics_command)
cli.add_command(zookeeper_group)
cli.add_command(stack_trace_command)
cli.add_command(chs3_backup_group)
cli.add_command(wait_started_command)
cli.add_command(disks_group)
cli.add_command(data_store_group)
cli.add_command(object_storage_group)
cli.add_command(config_command)


def main():
    cli()
