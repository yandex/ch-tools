import os
import time
from typing import Any, Optional, Tuple

from click import Context

from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.clickhouse.config.utils import dump_config

SUPPORTED_SAMPLE_TYPES = ["CPU", "Real", "MemorySample"]
FLAMEGRAPH_CLICKHOUSE_CONFIG_PATH_TEMPLATE = (
    "/etc/clickhouse-server/users.d/{trace_type}_flamegraph_config.xml"
)
SYSTEM_RELOAD_CONFIG_QUERY = "SYSTEM RELOAD CONFIG"


class ClickhouseTempFlamegraphConfigs:
    def __init__(self, ctx: Context, trace_type: str) -> None:
        self._ctx = ctx
        self._trace_type = trace_type

    def __enter__(self) -> None:
        setup_flamegraph_settings(self._ctx, self._trace_type)

    def __exit__(self, *args: Any) -> None:
        remove_flamegraph_settings(self._ctx)


def setup_flamegraph_settings(ctx: Context, trace_type: str) -> None:

    if trace_type not in SUPPORTED_SAMPLE_TYPES:
        raise NotImplementedError(f"Not implemented for type {trace_type}")

    config = ctx.obj["config"]["flamegraph"]["clickhouse_settings_per_sample_type"]
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
        trace_type_to_remove if trace_type_to_remove else SUPPORTED_SAMPLE_TYPES
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
