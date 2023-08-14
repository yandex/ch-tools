from collections import OrderedDict

from click import Choice, group, option, pass_context

from ch_tools.chadmin.internal.part import list_part_log
from ch_tools.common.cli.formatting import format_bytes, print_response

FIELD_FORMATTERS = {
    "size_in_bytes": format_bytes,
    "read_bytes": format_bytes,
    "peak_memory_usage": format_bytes,
}


@group("part-log")
def part_log_group():
    """
    Commands for retrieving information from system.part_log.
    """
    pass


@part_log_group.command("list")
@option("-d", "--database")
@option("-t", "--table")
@option("--partition")
@option("--part")
@option("--date")
@option("--min-date")
@option("--max-date")
@option("--min-time")
@option("--max-time")
@option("--time")
@option(
    "--order-by",
    type=Choice(["time", "size", "rows", "peak_memory_usage"]),
    default="time",
    help="Sorting order.",
)
@option(
    "-l",
    "--limit",
    type=int,
    default=10,
    help="Limit the max number of objects in the output.",
)
@pass_context
def list_part_log_command(
    ctx, date, min_date, max_date, min_time, max_time, time, **kwargs
):
    min_date = min_date or date
    max_date = max_date or date
    min_time = min_time or time
    max_time = max_time or time

    def _table_formatter(record):
        result = OrderedDict()
        result["event_time"] = record["event_time"]
        result["event_type"] = record["event_type"]
        result["completed"] = not record["exception"]
        result["duration_ms"] = record["duration_ms"]
        result["database"] = record["database"]
        result["table"] = record["table"]
        result["part_name"] = record["part_name"]
        result["rows"] = record["rows"]
        result["size"] = record["size_in_bytes"]
        result["peak_memory_usage"] = record["peak_memory_usage"]

        return result

    records = list_part_log(
        ctx,
        min_date=min_date,
        max_date=max_date,
        min_time=min_time,
        max_time=max_time,
        **kwargs,
    )

    print_response(
        ctx,
        records,
        default_format="table",
        field_formatters=FIELD_FORMATTERS,
        table_formatter=_table_formatter,
    )
