#!/usr/bin/env python3
import logging

import sqlparse
import xmltodict
from click import Choice, argument, group, option, pass_context

from cli.async_metrics import list_async_metrics_command
from cli.config import get_config_command
from cli.databases import database_group
from cli.system import system_group
from cli.diagnostics import diagnostics_command
from cli.dictionaries import list_dictionaries_command
from cli.events import list_events_command
from cli.functions import list_functions_command
from cli.macros import list_macros_command
from cli.metrics import list_metrics_command
from cli.mutations import mutation_group
from cli.part_log import part_log_group
from cli.partitions import partition_group
from cli.process import process_group
from cli.query_log import query_log_group
from cli.replication_queue import replication_queue_group
from cli.settings import list_settings_command
from cli.stack_trace import stack_trace_command
from cli.tables import table_group
from cli.thread_log import thread_log_group
from cli.users import user_group
from cli.zookeeper import zookeeper_group
from lib import execute


@group(context_settings=dict(help_option_names=['-h', '--help']))
@argument('host')
@option('-f', '--format', type=Choice(['json', 'yaml', 'table']), help="Output format.")
@option('-d', '--debug', is_flag=True, help="Enable debug output.")
@pass_context
def cli(ctx, host, format, debug):
    """ClickHouse management tool."""

    if debug:
        logging.basicConfig(level='DEBUG', format='%(levelname)s:%(message)s')

    chcli = ClickhouseClient(host)

    ctx.obj = dict(host=host, chcli=chcli, format=format, debug=debug)


cli.add_command(database_group)
cli.add_command(table_group)
cli.add_command(partition_group)
cli.add_command(mutation_group)
cli.add_command(query_log_group)
cli.add_command(thread_log_group)
cli.add_command(part_log_group)
cli.add_command(process_group)
cli.add_command(user_group)
cli.add_command(replication_queue_group)
cli.add_command(get_config_command)
cli.add_command(list_settings_command)
cli.add_command(list_functions_command)
cli.add_command(list_macros_command)
cli.add_command(list_dictionaries_command)
cli.add_command(list_events_command)
cli.add_command(list_metrics_command)
cli.add_command(list_async_metrics_command)
cli.add_command(zookeeper_group)
cli.add_command(diagnostics_command)
cli.add_command(stack_trace_command)
cli.add_command(system_group)


class ClickhouseClient:
    def __init__(self, host, user='mdb_admin'):
        self.host = host
        self.user = user

    def query(self, query, **kwargs):
        params = {}
        params['user'] = self.user
        params.update(kwargs)

        command = 'clickhouse-client ' + ' '.join('--{0} {1}'.format(k, v) for k, v in params.items() if v is not None)

        logging.debug('Executing query on %s:\n%s\n', self.host, sqlparse.format(query, reindent=True))
        return execute(host=self.host, command=command, input=query.encode())

    def config(self):
        data = execute(host=self.host, command='sudo cat /var/lib/clickhouse/preprocessed_configs/config.xml', )
        return xmltodict.parse(data)

    def users_config(self):
        data = execute(host=self.host, command='sudo cat /var/lib/clickhouse/preprocessed_configs/users.xml')
        return xmltodict.parse(data)


if __name__ == '__main__':
    cli()
