#!/usr/bin/env python3
import logging

from click import Choice, group, option, pass_context
from cloud.mdb.cli.common.parameters import TimeSpanParamType

from cloud.mdb.clickhouse.tools.chadmin.cli.access_migration_group import access_migration_group
from cloud.mdb.clickhouse.tools.chadmin.cli.dictionary_group import dictionary_group
from cloud.mdb.clickhouse.tools.chadmin.cli.list_async_metrics_command import list_async_metrics_command
from cloud.mdb.clickhouse.tools.chadmin.cli.crash_log_group import crash_log_group
from cloud.mdb.clickhouse.tools.chadmin.cli.database_group import database_group
from cloud.mdb.clickhouse.tools.chadmin.cli.merge_group import merge_group
from cloud.mdb.clickhouse.tools.chadmin.cli.part_group import part_group
from cloud.mdb.clickhouse.tools.chadmin.cli.list_events_command import list_events_command
from cloud.mdb.clickhouse.tools.chadmin.cli.list_functions_command import list_functions_command
from cloud.mdb.clickhouse.tools.chadmin.cli.list_macros_command import list_macros_command
from cloud.mdb.clickhouse.tools.chadmin.cli.list_metrics_command import list_metrics_command
from cloud.mdb.clickhouse.tools.chadmin.cli.mutation_group import mutation_group
from cloud.mdb.clickhouse.tools.chadmin.cli.part_log_group import part_log_group
from cloud.mdb.clickhouse.tools.chadmin.cli.partition_group import partition_group
from cloud.mdb.clickhouse.tools.chadmin.cli.process_group import process_group
from cloud.mdb.clickhouse.tools.chadmin.cli.query_log_group import query_log_group
from cloud.mdb.clickhouse.tools.chadmin.cli.replicated_fetch_group import replicated_fetch_group
from cloud.mdb.clickhouse.tools.chadmin.cli.replication_queue import replication_queue_group
from cloud.mdb.clickhouse.tools.chadmin.cli.restore_replica_command import restore_replica_command
from cloud.mdb.clickhouse.tools.chadmin.cli.list_settings_command import list_settings_command
from cloud.mdb.clickhouse.tools.chadmin.cli.stack_trace_command import stack_trace_command
from cloud.mdb.clickhouse.tools.chadmin.cli.table_replica_group import table_replica_group
from cloud.mdb.clickhouse.tools.chadmin.cli.table_group import table_group
from cloud.mdb.clickhouse.tools.chadmin.cli.thread_log_group import thread_log_group
from cloud.mdb.clickhouse.tools.chadmin.cli.wait_started_command import wait_started_command
from cloud.mdb.clickhouse.tools.chadmin.cli.zookeeper_group import zookeeper_group
from cloud.mdb.clickhouse.tools.chadmin.cli.chs3_backup_group import chs3_backup_group
from cloud.mdb.clickhouse.tools.chadmin.cli.disk_group import disks_group
from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseClient


@group(context_settings=dict(help_option_names=['-h', '--help'], terminal_width=120))
@option('-f', '--format', type=Choice(['json', 'yaml', 'table']), help="Output format.")
@option('--timeout', type=TimeSpanParamType(), help="Timeout for SQL queries.")
@option('--port', type=int, help="Port to connect.")
@option('-d', '--debug', is_flag=True, help="Enable debug output.")
@pass_context
def cli(ctx, format, timeout, port, debug):
    """ClickHouse administration tool."""

    if debug:
        logging.basicConfig(level='DEBUG', format='%(levelname)s:%(message)s')

    timeout_seconds = timeout.total_seconds() if timeout else None
    chcli = ClickhouseClient(timeout=timeout_seconds, port=port)

    ctx.obj = dict(chcli=chcli, format=format, debug=debug)


cli.add_command(access_migration_group)
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


def main():
    cli()
