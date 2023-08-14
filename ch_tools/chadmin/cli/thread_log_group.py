from click import argument, group, option, pass_context

from ch_tools.chadmin.internal.utils import execute_query


@group("thread-log")
def thread_log_group():
    """
    Commands for retrieving information from system.query_thread_log.
    """
    pass


@thread_log_group.command("list")
@argument("query_id")
@option("--date")
@option("--min-date")
@option("--max-date")
@option("--min-time")
@option("--max-time")
@option("-v", "--verbose", is_flag=True, help="Verbose mode.")
@pass_context
def list_threads_command(
    ctx, query_id, date, min_date, max_date, min_time, max_time, verbose
):
    min_date = min_date or date
    max_date = max_date or date
    print(
        get_threads(
            ctx,
            query_id=query_id,
            min_date=min_date,
            max_date=max_date,
            min_time=min_time,
            max_time=max_time,
            verbose=verbose,
        )
    )


def get_threads(
    ctx,
    query_id=None,
    min_date=None,
    max_date=None,
    min_time=None,
    max_time=None,
    verbose=False,
):
    query_str = """
        SELECT
             query_id,
             thread_name,
             thread_number,
             concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) "read",
             concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) "written",
             formatReadableSize(memory_usage) "memory_usage",
             formatReadableSize(peak_memory_usage) "peak_memory_usage",
        {% if not verbose %}
             master_thread_number
        {% else %}
             master_thread_number,
             ProfileEvents.Names,
             ProfileEvents.Values
        {% endif %}
        FROM system.query_thread_log
        WHERE query_id = '{{ query_id }}'
        {% if min_date %}
          AND event_date >= toDate('{{ min_date }}')
        {% endif %}
        {% if max_date %}
          AND event_date <= toDate('{{ max_date }}')
        {% endif %}
        {% if min_time %}
          AND event_date >= toDate('{{ min_time }}') AND event_time >= toDateTime('{{ min_time }}')
        {% endif %}
        {% if max_time %}
          AND event_date <= toDate('{{ max_time }}') AND event_time <= toDateTime('{{ max_time }}')
        {% endif %}
        {% if not min_date and not max_date and not min_time and not max_time %}
          AND event_date = today()
        {% endif %}
        {% if query_id %}
        {% endif %}
        """
    return execute_query(
        ctx,
        query_str,
        query_id=query_id,
        min_date=min_date,
        max_date=max_date,
        min_time=min_time,
        max_time=max_time,
        verbose=verbose,
        format_="Vertical",
    )


@thread_log_group.command("get-metrics")
@argument("query_id")
@option("--date")
@option("--min-date")
@option("--max-date")
@option("--min-time")
@option("--max-time")
@pass_context
def get_thread_metrics_command(
    ctx, query_id, date, min_date, max_date, min_time, max_time
):
    min_date = min_date or date
    max_date = max_date or date
    query_str = """
        SELECT
             thread_name,
             thread_number,
             ProfileEvents.Names "name",
             ProfileEvents.Values "value"
        FROM system.query_thread_log
        ARRAY JOIN ProfileEvents
        WHERE query_id = '{{ query_id }}'
        {% if min_date %}
          AND event_date >= toDate('{{ min_date }}')
        {% endif %}
        {% if max_date %}
          AND event_date <= toDate('{{ max_date }}')
        {% endif %}
        {% if min_time %}
          AND event_date >= toDate('{{ min_time }}') AND event_time >= toDateTime('{{ min_time }}')
        {% endif %}
        {% if max_time %}
          AND event_date <= toDate('{{ max_time }}') AND event_time <= toDateTime('{{ max_time }}')
        {% endif %}
        {% if not min_date and not max_date and not min_time and not max_time %}
          AND event_date = today()
        {% endif %}
        ORDER BY thread_name, thread_number, name
        """
    print(
        execute_query(
            ctx,
            query_str,
            query_id=query_id,
            min_date=min_date,
            max_date=max_date,
            min_time=min_time,
            max_time=max_time,
        )
    )
