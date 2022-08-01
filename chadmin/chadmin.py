#!/usr/bin/env python3
import logging

from click import Choice, group, option, pass_context

from cloud.mdb.clickhouse.tools.chadmin.cli.dictionary_group import dictionary_group
from cloud.mdb.clickhouse.tools.chadmin.cli.async_metrics import list_async_metrics_command
from cloud.mdb.clickhouse.tools.chadmin.cli.crash_log import crash_log_group
from cloud.mdb.clickhouse.tools.chadmin.cli.databases import database_group
from cloud.mdb.clickhouse.tools.chadmin.cli.parts import part_group
from cloud.mdb.clickhouse.tools.chadmin.cli.events import list_events_command
from cloud.mdb.clickhouse.tools.chadmin.cli.functions import list_functions_command
from cloud.mdb.clickhouse.tools.chadmin.cli.macros import list_macros_command
from cloud.mdb.clickhouse.tools.chadmin.cli.metrics import list_metrics_command
from cloud.mdb.clickhouse.tools.chadmin.cli.mutations import mutation_group
from cloud.mdb.clickhouse.tools.chadmin.cli.part_log import part_log_group
from cloud.mdb.clickhouse.tools.chadmin.cli.partitions import partition_group
from cloud.mdb.clickhouse.tools.chadmin.cli.process import process_group
from cloud.mdb.clickhouse.tools.chadmin.cli.query_log import query_log_group
from cloud.mdb.clickhouse.tools.chadmin.cli.replication_queue import replication_queue_group
from cloud.mdb.clickhouse.tools.chadmin.cli.restore_replica import restore_replica_command
from cloud.mdb.clickhouse.tools.chadmin.cli.settings import list_settings_command
from cloud.mdb.clickhouse.tools.chadmin.cli.stack_trace import stack_trace_command
from cloud.mdb.clickhouse.tools.chadmin.cli.table_replica_group import table_replica_group
from cloud.mdb.clickhouse.tools.chadmin.cli.tables import table_group
from cloud.mdb.clickhouse.tools.chadmin.cli.thread_log import thread_log_group
from cloud.mdb.clickhouse.tools.chadmin.cli.wait_started import wait_started_command
from cloud.mdb.clickhouse.tools.chadmin.cli.zookeeper import zookeeper_group
from cloud.mdb.clickhouse.tools.chadmin.cli.chs3_backup import chs3_backup_group
from cloud.mdb.clickhouse.tools.chadmin.cli.disks import disks_group
from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseClient


@group(context_settings=dict(help_option_names=['-h', '--help']))
@option('-f', '--format', type=Choice(['json', 'yaml', 'table']), help="Output format.")
@option('-d', '--debug', is_flag=True, help="Enable debug output.")
@pass_context
def cli(ctx, format, debug):
    """ClickHouse administration tool."""

    if debug:
        logging.basicConfig(level='DEBUG', format='%(levelname)s:%(message)s')

    chcli = ClickhouseClient()

    ctx.obj = dict(chcli=chcli, format=format, debug=debug)


cli.add_command(database_group)
cli.add_command(table_group)
cli.add_command(table_replica_group)
cli.add_command(partition_group)
cli.add_command(part_group)
cli.add_command(dictionary_group)
cli.add_command(mutation_group)
cli.add_command(query_log_group)
cli.add_command(thread_log_group)
cli.add_command(part_log_group)
cli.add_command(crash_log_group)
cli.add_command(process_group)
cli.add_command(replication_queue_group)
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
