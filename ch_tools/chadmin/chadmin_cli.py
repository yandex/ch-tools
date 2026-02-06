#!/usr/bin/env python3
import warnings
from datetime import timedelta
from typing import Any, List

import cloup
from click import Context

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.cli.move_group import move_group
from ch_tools.common.config import load_config
from ch_tools.common.utils import update_by_key_path

warnings.filterwarnings(action="ignore", message="Python 3.6 is no longer supported")
warnings.filterwarnings(
    action="ignore", message="Boto3 will no longer support Python 3.6"
)

# pylint: disable=wrong-import-position

from ch_tools import __version__
from ch_tools.chadmin.cli.chs3_backup_group import chs3_backup_group
from ch_tools.chadmin.cli.config_command import config_command
from ch_tools.chadmin.cli.crash_log_group import crash_log_group
from ch_tools.chadmin.cli.data_store_group import data_store_group
from ch_tools.chadmin.cli.database_group import database_group
from ch_tools.chadmin.cli.diagnostics_command import diagnostics_command
from ch_tools.chadmin.cli.dictionary_group import dictionary_group
from ch_tools.chadmin.cli.disk_group import disks_group
from ch_tools.chadmin.cli.flamegraph_group import flamegraph_group
from ch_tools.chadmin.cli.list_async_metrics_command import list_async_metrics_command
from ch_tools.chadmin.cli.list_events_command import list_events_command
from ch_tools.chadmin.cli.list_functions_command import list_functions_command
from ch_tools.chadmin.cli.list_macros_command import list_macros_command
from ch_tools.chadmin.cli.list_metrics_command import list_metrics_command
from ch_tools.chadmin.cli.list_settings_command import list_settings_command
from ch_tools.chadmin.cli.merge_group import merge_group
from ch_tools.chadmin.cli.mutation_group import mutation_group
from ch_tools.chadmin.cli.object_storage_group import object_storage_group
from ch_tools.chadmin.cli.part_group import part_group
from ch_tools.chadmin.cli.part_log_group import part_log_group
from ch_tools.chadmin.cli.partition_group import partition_group
from ch_tools.chadmin.cli.process_group import process_group
from ch_tools.chadmin.cli.query_log_group import query_log_group
from ch_tools.chadmin.cli.replica_group import replica_group
from ch_tools.chadmin.cli.replicated_fetch_group import replicated_fetch_group
from ch_tools.chadmin.cli.replication_queue_group import replication_queue_group
from ch_tools.chadmin.cli.restore_replica_command import restore_replica_command
from ch_tools.chadmin.cli.s3_credentials_config_group import s3_credentials_config_group
from ch_tools.chadmin.cli.server_group import server_group
from ch_tools.chadmin.cli.stack_trace_command import stack_trace_command
from ch_tools.chadmin.cli.table_group import table_group
from ch_tools.chadmin.cli.thread_log_group import thread_log_group
from ch_tools.chadmin.cli.wait_group import wait_group
from ch_tools.chadmin.cli.zookeeper_group import zookeeper_group
from ch_tools.common.cli.context_settings import CONTEXT_SETTINGS
from ch_tools.common.cli.locale_resolver import LocaleResolver
from ch_tools.common.cli.parameters import TimeSpanParamType, YamlParamType


@cloup.group(
    "chadmin",
    cls=Chadmin,
    help="ClickHouse administration tool.",
    context_settings=CONTEXT_SETTINGS,
)
@cloup.option(
    "-f",
    "--format",
    "format_",
    type=cloup.Choice(["json", "yaml", "table"]),
    help="Output format.",
)
@cloup.option(
    "--setting",
    "settings",
    multiple=True,
    type=(str, YamlParamType()),
    metavar="NAME VALUE",
    help="Name and value of tool setting to override. "
    "Can be specified multiple times to override several settings.",
)
@cloup.option("--timeout", type=TimeSpanParamType(), help="Timeout for SQL queries.")
@cloup.option("--port", type=int, help="Port to connect.")
@cloup.option("-d", "--debug", is_flag=True, help="Enable debug output.")
@cloup.version_option(__version__)
@cloup.pass_context
def cli(
    ctx: Context,
    format_: str,
    settings: dict,
    timeout: timedelta,
    port: int,
    debug: bool,
) -> None:
    """ClickHouse administration tool."""
    config = load_config()

    for setting_path, value in settings:
        update_by_key_path(config, setting_path, value)

    if port:
        config["clickhouse"]["port"] = port

    if timeout:
        config["clickhouse"]["timeout"] = timeout.total_seconds()

    ctx.obj = {
        "config": config,
        "format": format_,
        "debug": debug,
    }
    ctx.default_map = config["chadmin"]


commands: List[Any] = [
    config_command,
    diagnostics_command,
    data_store_group,
    list_async_metrics_command,
    list_events_command,
    list_functions_command,
    list_macros_command,
    list_metrics_command,
    list_settings_command,
    restore_replica_command,
    stack_trace_command,
]

groups: List[Any] = [
    chs3_backup_group,
    crash_log_group,
    data_store_group,
    database_group,
    dictionary_group,
    disks_group,
    merge_group,
    move_group,
    mutation_group,
    object_storage_group,
    part_group,
    part_log_group,
    s3_credentials_config_group,
    partition_group,
    process_group,
    query_log_group,
    replicated_fetch_group,
    replication_queue_group,
    server_group,
    table_group,
    replica_group,
    thread_log_group,
    wait_group,
    zookeeper_group,
    flamegraph_group,
]

section = cloup.Section("Commands")
for command in commands:
    cli.add_command(command, section=section)
section = cloup.Section("Groups")
for group in groups:
    cli.add_group(group, section=section)


def main() -> None:
    """
    Program entry point.
    """
    LocaleResolver.resolve()
    cli.main()


if __name__ == "__main__":
    main()
