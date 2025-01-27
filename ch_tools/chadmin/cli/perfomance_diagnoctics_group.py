import os
import time
from datetime import timedelta
from typing import Optional, Tuple

from click import Choice, Context, group, option, pass_context

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.cli.parameters import StringParamType, TimeSpanParamType
from ch_tools.common.clickhouse.config.utils import dump_config


@group("perfomance-dianostics", cls=Chadmin)
def perfomance_dianostics_group():
    """
    Commands for collecting the flamegraph.

    There are two options how to collect traces:
    1. Use `collect-flamegraph-by-interval` to collect the ch-server-wide data.
    2. Use 4 steps:
        * run setup-ch-settings-for-flamegraph to setup all required settings for collecting.
        * run the interesting query in clickhouse and keep it's query_id
        * run `collect-flame-graph-by-query-id` to collect flamegraph for single query.
        * run `remove-ch-settings-for-flamegraph` to remove temp settings. It's important!
          Otherwise, traces will continue to be collected at a high sampling rate, which will increase the load.

    To render the image with flamegraph, [flamegraph.pl utility](https://github.com/brendangregg/FlameGraph) can be used.
    """
    pass


## TODO: Handle other types of samples.
SUPPORTED_SAMLE_TYPES = ["CPU", "MemorySample"]
FLAMEGRAPH_CLICKHOUSE_CONFIG_PATH_TEMPLATE = (
    "/etc/clickhouse-server/users.d/{trace_type}_flamegraph_config.xml"
)
SYSTEM_RELOAD_CONFIG_QUERY = "SYSTEM RELOAD CONFIG"


@perfomance_dianostics_group.command("collect-flamegraph-by-interval")
@pass_context
@option(
    "--trace-type",
    "trace_type",
    type=Choice(SUPPORTED_SAMLE_TYPES),
    help="Trace type to build flamegraph.",
)
@option(
    "--collect-interval",
    "collect_inverval",
    type=TimeSpanParamType(),
    default="10s",
    help="How many time collec traces.",
)
def collect_flamegraph_by_interval(
    ctx: Context, trace_type: str, collect_inverval: timedelta
) -> None:
    with ClickhouseTempFlamegraphConfigs(ctx, trace_type):
        if collect_inverval:
            start_event_time = execute_query(ctx, "SELECT now()", format_=None)
            time.sleep(collect_inverval.total_seconds())
            end_event_time = execute_query(ctx, "SELECT now()", format_=None)
            collect_flamegraph(
                ctx, trace_type, time_interval=(start_event_time, end_event_time)
            )


@perfomance_dianostics_group.command("setup-ch-settings-for-flamegraph")
@pass_context
@option(
    "--trace-type",
    "trace_type",
    type=Choice(SUPPORTED_SAMLE_TYPES),
    help="Trace type to build the flamegraph. Required here to setup needed settings for the ch-server.",
)
def setup_clickhouse_settings_flamegraph(ctx: Context, trace_type: str) -> None:
    setup_flamegraph_settings(ctx, trace_type)


@perfomance_dianostics_group.command("remove-ch-settings-for-flamegraph")
@pass_context
@option(
    "--trace-type",
    "trace_type",
    type=Choice(SUPPORTED_SAMLE_TYPES),
    help="Trace type to build the flamegraph.",
)
def remove_clickhouse_settings_flamegraph(ctx: Context, trace_type: str) -> None:
    remove_flamegraph_settings(ctx, trace_type)


@perfomance_dianostics_group.command("collect-flame-graph-by-query-id")
@pass_context
@option(
    "--trace-type",
    "trace_type",
    type=Choice(SUPPORTED_SAMLE_TYPES),
    help="Trace type to build the flamegraph.",
)
@option(
    "--query-id",
    "query_id",
    type=StringParamType(),
    help="Query_id to build the flamegraph .",
)
def collect_flame_graph_by_query_id(
    ctx: Context, trace_type: str, query_id: str
) -> None:
    collect_flamegraph(ctx, trace_type, query_id=query_id)


class ClickhouseTempFlamegraphConfigs:
    def __init__(self, ctx: Context, trace_type: str) -> None:
        self._ctx = ctx
        self._trace_type = trace_type

    def __enter__(self):
        setup_flamegraph_settings(self._ctx, self._trace_type)

    def __exit__(self, *args):
        remove_flamegraph_settings(self._ctx)


def setup_flamegraph_settings(ctx: Context, trace_type: str) -> None:

    if trace_type not in SUPPORTED_SAMLE_TYPES:
        raise NotImplementedError(f"Not implemented for type {trace_type}")

    config = ctx.obj["config"]["perfomance_diagnoctics_group"][
        "clickhouse_settings_per_sample_type"
    ]
    settings = {"clickhouse": {"profiles": {"default": config[trace_type]}}}

    config_content_xml = dump_config(settings, xml_format=True)
    flamegraph_config_path = FLAMEGRAPH_CLICKHOUSE_CONFIG_PATH_TEMPLATE.format(
        trace_type=trace_type
    )
    with open(flamegraph_config_path, "w", encoding="utf-8") as config:
        config.write(config_content_xml)

    execute_query(ctx, SYSTEM_RELOAD_CONFIG_QUERY, format_=None)


def remove_flamegraph_settings(
    ctx: Context, trace_type_to_remove: Optional[str] = None
) -> None:
    to_remove = [
        trace_type_to_remove if trace_type_to_remove else SUPPORTED_SAMLE_TYPES
    ]
    for trace_type in to_remove:
        try:
            os.remove(
                FLAMEGRAPH_CLICKHOUSE_CONFIG_PATH_TEMPLATE.format(trace_type=trace_type)
            )
        except FileNotFoundError:
            pass
    execute_query(ctx, SYSTEM_RELOAD_CONFIG_QUERY, format_=None)


def collect_flamegraph(
    ctx: Context,
    trace_type: str,
    query_id: Optional[str] = None,
    time_interval: Optional[Tuple[str, str]] = None,
) -> None:

    if not query_id and not time_interval:
        raise ValueError(
            "To collect flamegraph, you must specify the query id or time interval."
        )

    query = """
        SELECT
            arrayJoin(flameGraph(trace {%if trace_type=='MemorySample' %} , size {% endif %}))
        {% if trace_type=='' %}
            arrayJoin(flameGraph(trace, size))
        {% endif %}
        FROM system.trace_log
        WHERE
            trace_type='{{ trace_type }}'
        {% if query_id -%}
            AND query_id='{{ query_id }}'
        {% endif %}
        {% if time_interval -%}
            AND event_time>='{{ time_interval[0] }}' AND event_time <= '{{ time_interval[1] }}'
        {% endif %}
        SETTINGS allow_introspection_functions=1
        """

    response = execute_query(
        ctx,
        query,
        format_=None,
        trace_type=trace_type,
        time_interval=time_interval,
        query_id=query_id,
    )
    filename = f"flamegraph-{trace_type}-{query_id if query_id else f'{time_interval[0]}-{time_interval[1]}'}.{time.time()}"  # type: ignore
    ## time interval values contains spaces, remove them
    filename = filename.replace(" ", "_")
    with open(filename, "w", encoding="utf-8") as file:
        file.write(response)
    logging.info("Flamegraph collected. Filename {}", filename)
