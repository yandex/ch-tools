#!/usr/bin/env python3
import logging

from click import Choice, group, option, pass_context
from chadmin.cli.config_command import config_command
from common.cli.parameters import TimeSpanParamType
from chadmin.cli.chs3_backup_group import chs3_backup_group
from chadmin.cli.crash_log_group import crash_log_group
from chadmin.cli.data_store_group import data_store_group
from chadmin.cli.database_group import database_group
from chadmin.cli.dictionary_group import dictionary_group
from chadmin.cli.disk_group import disks_group
from chadmin.cli.list_async_metrics_command import list_async_metrics_command
from chadmin.cli.list_events_command import list_events_command
from chadmin.cli.list_functions_command import list_functions_command
from chadmin.cli.list_macros_command import list_macros_command
from chadmin.cli.list_metrics_command import list_metrics_command
from chadmin.cli.list_settings_command import list_settings_command
from chadmin.cli.merge_group import merge_group
from chadmin.cli.mutation_group import mutation_group
from chadmin.cli.object_storage_group import object_storage_group
from chadmin.cli.part_group import part_group
from chadmin.cli.part_log_group import part_log_group
from chadmin.cli.partition_group import partition_group
from chadmin.cli.process_group import process_group
from chadmin.cli.query_log_group import query_log_group
from chadmin.cli.replicated_fetch_group import replicated_fetch_group
from chadmin.cli.replication_queue import replication_queue_group
from chadmin.cli.restore_replica_command import restore_replica_command
from chadmin.cli.stack_trace_command import stack_trace_command
from chadmin.cli.table_group import table_group
from chadmin.cli.table_replica_group import table_replica_group
from chadmin.cli.thread_log_group import thread_log_group
from chadmin.cli.wait_started_command import wait_started_command
from chadmin.cli.zookeeper_group import zookeeper_group

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

    chcliconf = dict(port=port, timeout=timeout_seconds, settings=settings)

    ctx.obj = dict(chcli_conf=chcliconf, format=format, debug=debug)


for cmd in (
    database_group,
    table_group,
    table_replica_group,
    partition_group,
    part_group,
    dictionary_group,
    merge_group,
    mutation_group,
    query_log_group,
    thread_log_group,
    part_log_group,
    crash_log_group,
    process_group,
    replication_queue_group,
    replicated_fetch_group,
    restore_replica_command,
    list_settings_command,
    list_functions_command,
    list_macros_command,
    list_events_command,
    list_metrics_command,
    list_async_metrics_command,
    zookeeper_group,
    stack_trace_command,
    chs3_backup_group,
    wait_started_command,
    disks_group,
    data_store_group,
    object_storage_group,
    config_command,
):
    cli.add_command(cmd)


def main():
    cli()
