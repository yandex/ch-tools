import time
from datetime import timedelta

from click import Choice, Context, group, option, pass_context

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.diagnostics.flamegraph import (
    SUPPORTED_SAMPLE_TYPES,
    ClickhouseTempFlamegraphConfigs,
    collect_flamegraph,
    remove_flamegraph_settings,
    setup_flamegraph_settings,
)
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common.cli.parameters import StringParamType, TimeSpanParamType


@group("flamegraph", cls=Chadmin)
def flamegraph_group():
    """
    Commands for collecting the flamegraph.

    There are two options how to collect traces:
    1. Use `collect-by-interval` to collect the ch-server-wide data.
    2. Use 4 steps:
        * run `setup` to setup all required settings for collecting.
        * run the interesting query in clickhouse and keep it's query_id
        * run `collect-by-query` to collect flamegraph for single query.
        * run `cleanup` to remove temp settings. It's important!
          Otherwise, traces will continue to be collected at a high sampling rate, which will increase the load.

    To render the image with flamegraph, [flamegraph.pl utility](https://github.com/brendangregg/FlameGraph) can be used.
    """
    pass


@flamegraph_group.command("collect-by-interval")
@pass_context
@option(
    "--trace-type",
    "trace_type",
    type=Choice(SUPPORTED_SAMPLE_TYPES),
    help="Trace type to build flamegraph.",
)
@option(
    "--collect-interval",
    "collect_inverval",
    type=TimeSpanParamType(),
    default="10s",
    help="How many time collect traces.",
)
def collect_by_interval(
    ctx: Context, trace_type: str, collect_inverval: timedelta
) -> None:
    """
    Collect flamegraph by time interval.
    """
    with ClickhouseTempFlamegraphConfigs(ctx, trace_type):
        if collect_inverval:
            start_event_time = execute_query(ctx, "SELECT now()", format_=None)
            time.sleep(collect_inverval.total_seconds())
            end_event_time = execute_query(ctx, "SELECT now()", format_=None)
            collect_flamegraph(
                ctx, trace_type, time_interval=(start_event_time, end_event_time)
            )


@flamegraph_group.command("setup")
@pass_context
@option(
    "--trace-type",
    "trace_type",
    type=Choice(SUPPORTED_SAMPLE_TYPES),
    help="Trace type to build the flamegraph. Required here to setup needed settings for the ch-server.",
)
def setup_clickhouse_settings_flamegraph(ctx: Context, trace_type: str) -> None:
    """
    Setup clickhouse config for collecting flamegraph.
    """
    setup_flamegraph_settings(ctx, trace_type)


@flamegraph_group.command("cleanup")
@pass_context
@option(
    "--trace-type",
    "trace_type",
    type=Choice(SUPPORTED_SAMPLE_TYPES),
    help="Trace type to build the flamegraph.",
)
def cleanup_clickhouse_settings_flamegraph(ctx: Context, trace_type: str) -> None:
    """
    Remove clickhouse config for collecting flamegraph.
    """
    remove_flamegraph_settings(ctx, trace_type)


@flamegraph_group.command("collect-by-query")
@pass_context
@option(
    "--trace-type",
    "trace_type",
    type=Choice(SUPPORTED_SAMPLE_TYPES),
    help="Trace type to build the flamegraph.",
)
@option(
    "--query-id",
    "query_id",
    type=StringParamType(),
    help="Query_id to build the flamegraph .",
)
def collect_by_query(ctx: Context, trace_type: str, query_id: str) -> None:
    """
    Collects flamegraph for specific query-id.
    """
    collect_flamegraph(ctx, trace_type, query_id=query_id)
