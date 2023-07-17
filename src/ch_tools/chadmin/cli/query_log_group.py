import datetime

from click import Choice, argument, group, option, pass_context

from ch_tools.chadmin.cli import get_cluster_name
from ch_tools.chadmin.internal.utils import execute_query


@group("query-log")
def query_log_group():
    """
    Commands for retrieving information from system.query_log.
    """
    pass


@query_log_group.command("get")
@argument("query_id")
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Search for log record on all hosts of the cluster.",
)
@pass_context
def get_query_command(ctx, **kwargs):
    print(get_queries(ctx, **kwargs, verbose=True))
    print("\nProfileEvents:")
    print(get_query_metrics(ctx, **kwargs))
    print("\nSettings:")
    print(get_query_settings(ctx, **kwargs))


@query_log_group.command("list")
@option("-u", "--user", help="Filter log records to output by user.")
@option("-U", "--exclude-user", help="Filter log records to not output by user.")
@option(
    "--query", "query_pattern", help="Filter log records to output by query pattern."
)
@option(
    "--exclude-query",
    "exclude_query_pattern",
    help="Filter log records to not output by query pattern.",
)
@option("--error")
@option("--date")
@option("--min-date")
@option("--max-date")
@option("--min-time")
@option("--max-time")
@option("--time")
@option("--client")
@option("--failed", is_flag=True)
@option("--completed", is_flag=True)
@option("--is-initial-query", "--initial", "is_initial_query", type=bool)
@option("-v", "--verbose", is_flag=True, help="Verbose mode.")
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Get log records from all hosts in the cluster.",
)
@option(
    "--order-by",
    type=Choice(
        [
            "query_start_time",
            "query_duration_ms",
            "memory_usage",
            "read_rows",
            "written_rows",
            "result_rows",
        ]
    ),
    default="query_start_time",
)
@option(
    "-l",
    "--limit",
    type=int,
    default=10,
    help="Limit the max number of objects in the output.",
)
@pass_context
def list_queries_command(
    ctx, date, min_date, max_date, min_time, max_time, time, **kwargs
):
    if not any((date, min_date, max_date, time, min_time, max_time)):
        date = datetime.date.today().isoformat()

    min_date = min_date or date
    max_date = max_date or date
    min_time = min_time or time
    max_time = max_time or time
    print(
        get_queries(
            ctx,
            min_date=min_date,
            max_date=max_date,
            min_time=min_time,
            max_time=max_time,
            **kwargs,
        )
    )


@query_log_group.command("get-statistics")
@option("-u", "--user")
@option("-U", "--exclude-user")
@option("--query", "query_pattern")
@option("--error")
@option("--date")
@option("--min-date")
@option("--max-date")
@option("--min-time")
@option("--max-time")
@option("--time")
@option("--failed", is_flag=True)
@pass_context
def get_statistics_command(
    ctx,
    user,
    exclude_user,
    query_pattern,
    error,
    date,
    min_date,
    max_date,
    min_time,
    max_time,
    time,
    failed,
):
    min_date = min_date or date
    max_date = max_date or date
    min_time = min_time or time
    max_time = max_time or time
    query_str = """
        SELECT
            count() "queries",
            uniq(query) "unique queries",
            countIf(exception = '') "completed queries",
            countIf(exception != '') "failed queries",
            concat(toString(sum(read_rows)), ' rows / ', formatReadableSize(sum(read_bytes))) "read (total)",
            concat(toString(round(avg(read_rows))), ' rows / ', formatReadableSize(avg(read_bytes))) "read (avg)",
            concat(toString(sum(written_rows)), ' rows / ', formatReadableSize(sum(written_bytes))) "written (total)",
            concat(toString(round(avg(written_rows))), ' rows / ', formatReadableSize(avg(written_bytes))) "written (avg)",
            concat(toString(sum(result_rows)), ' rows / ', formatReadableSize(sum(result_bytes))) "result (total)",
            concat(toString(round(avg(result_rows))), ' rows / ', formatReadableSize(avg(result_bytes))) "result (avg)",
            formatReadableSize(sum(memory_usage)) "memory usage (total)",
            formatReadableSize(avg(memory_usage)) "memory usage (avg)"
        FROM system.query_log
        WHERE type != 1
        {% if min_date -%}
          AND event_date >= toDate('{{ min_date }}')
        {% elif min_time -%}
          AND event_date >= toDate('{{ min_time }}')
        {% endif -%}
        {% if max_date -%}
          AND event_date <= toDate('{{ max_date }}')
        {% elif max_time -%}
          AND event_date <= toDate('{{ max_time }}')
        {% endif -%}
        {% if not min_date and not max_date and not min_time and not max_time -%}
          AND event_date = today()
        {% endif -%}
        {% if min_time -%}
          AND event_time >= toDateTime('{{ min_time }}')
        {% endif -%}
        {% if max_time -%}
          AND query_start_time <= toDateTime('{{ max_time }}')
        {% endif -%}
        {% if user -%}
          AND user = '{{ user }}'
        {% endif -%}
        {% if exclude_user -%}
          AND user != '{{ exclude_user }}'
        {% endif -%}
        {% if query_pattern -%}
          AND lower(query) LIKE lower('{{ query_pattern }}')
        {% endif -%}
        {% if failed -%}
          AND exception != ''
        {% endif -%}
        {% if error -%}
          AND lower(exception) LIKE lower('{{ error }}')
        {% endif -%}
    """
    print(
        execute_query(
            ctx,
            query_str,
            user=user,
            exclude_user=exclude_user,
            query_pattern=query_pattern,
            error=error,
            min_date=min_date,
            max_date=max_date,
            min_time=min_time,
            max_time=max_time,
            failed=failed,
            format_="Vertical",
        )
    )


@query_log_group.command("get-settings")
@argument("query_id")
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Search for log record on all hosts of the cluster.",
)
@pass_context
def get_query_settings_command(ctx, query_id, on_cluster):
    cluster = get_cluster_name(ctx) if on_cluster else None
    query_str = """
        SELECT
             Settings.Names "name",
             Settings.Values "value"
        {% if cluster -%}
        FROM clusterAllReplicas({{ cluster }}, system.query_log)
        {% else -%}
        FROM system.query_log
        {% endif -%}
        ARRAY JOIN Settings
        WHERE type != 1
          AND query_id = '{{ query_id }}'
        """
    print(execute_query(ctx, query_str, query_id=query_id, cluster=cluster))


@query_log_group.command("get-metrics")
@argument("query_id")
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Search for log record on all hosts of the cluster.",
)
@pass_context
def get_query_metrics_command(ctx, query_id, on_cluster):
    cluster = get_cluster_name(ctx) if on_cluster else None
    query_str = """
        SELECT
             ProfileEvents.Names "name",
             ProfileEvents.Values "value"
        {% if cluster -%}
        FROM clusterAllReplicas({{ cluster }}, system.query_log)
        {% else -%}
        FROM system.query_log
        {% endif -%}
        ARRAY JOIN ProfileEvents
        WHERE type != 1
          AND query_id = '{{ query_id }}'
        ORDER BY name
        """
    print(execute_query(ctx, query_str, query_id=query_id, cluster=cluster))


def get_queries(
    ctx,
    user=None,
    exclude_user=None,
    query_id=None,
    query_pattern=None,
    exclude_query_pattern=None,
    error=None,
    min_date=None,
    max_date=None,
    min_time=None,
    max_time=None,
    client=None,
    failed=None,
    completed=None,
    is_initial_query=None,
    on_cluster=False,
    limit=10,
    order_by="query_start_time",
    verbose=False,
):
    cluster = get_cluster_name(ctx) if on_cluster else None
    query_str = """
        SELECT
             {% if cluster -%}
             hostName() "host",
             {% endif -%}
             query_start_time,
             query_duration_ms,
             query_id,
             is_initial_query,
             query,
             concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) "read",
             concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) "written",
             concat(toString(result_rows), ' rows / ', formatReadableSize(result_bytes)) "result",
             formatReadableSize(memory_usage) "memory usage",
             user,
             multiIf(empty(client_name),
                     http_user_agent,
                     concat(client_name, ' ',
                            toString(client_version_major), '.',
                            toString(client_version_minor), '.',
                            toString(client_version_patch))) "client",
             client_hostname,
             {% if not verbose -%}
             exception
             {%- else %}
             address,
             exception,
             stack_trace
             {%- endif %}
        {% if cluster -%}
        FROM clusterAllReplicas({{ cluster }}, system.query_log)
        {% else -%}
        FROM system.query_log
        {% endif -%}
        WHERE type != 1
        {% if min_date -%}
          AND event_date >= toDate('{{ min_date }}')
        {% elif min_time -%}
          AND event_date >= toDate('{{ min_time }}')
        {% endif -%}
        {% if max_date -%}
          AND event_date <= toDate('{{ max_date }}')
        {% elif max_time -%}
          AND event_date <= toDate('{{ max_time }}')
        {% endif -%}
        {% if min_time -%}
          AND event_time >= toDateTime('{{ min_time }}')
        {% endif -%}
        {% if max_time -%}
          AND query_start_time <= toDateTime('{{ max_time }}')
        {% endif -%}
        {% if client -%}
          AND client = '{{ client }}'
        {% endif -%}
        {% if user -%}
          AND user = '{{ user }}'
        {% endif -%}
        {% if exclude_user -%}
          AND user != '{{ exclude_user }}'
        {% endif -%}
        {% if query_id -%}
          AND query_id = '{{ query_id }}'
        {% endif -%}
        {% if query_pattern -%}
          AND lower(query) LIKE lower('{{ query_pattern }}')
        {% endif -%}
        {% if exclude_query_pattern -%}
          AND lower(query) NOT LIKE lower('{{ exclude_query_pattern }}')
        {% endif -%}
        {% if failed -%}
          AND exception != ''
        {% endif -%}
        {% if completed -%}
          AND exception = ''
        {% endif -%}
        {% if is_initial_query is true -%}
          AND is_initial_query = 1
        {% elif is_initial_query is false -%}
          AND is_initial_query = 0
        {% endif -%}
        {% if error -%}
          AND lower(exception) LIKE lower('{{ error }}')
        {% endif -%}
        {% if not query_id -%}
        ORDER BY {{ order_by }} DESC
        {% endif -%}
        {% if limit %}
        LIMIT {{ limit }}
        {% endif %}
        """
    return execute_query(
        ctx,
        query_str,
        user=user,
        exclude_user=exclude_user,
        query_id=query_id,
        query_pattern=query_pattern,
        exclude_query_pattern=exclude_query_pattern,
        error=error,
        min_date=min_date,
        max_date=max_date,
        min_time=min_time,
        max_time=max_time,
        client=client,
        failed=failed,
        completed=completed,
        is_initial_query=is_initial_query,
        cluster=cluster,
        limit=limit,
        verbose=verbose,
        order_by=order_by,
        format_="Vertical",
    )


def get_query_settings(ctx, query_id, on_cluster=False):
    cluster = get_cluster_name(ctx) if on_cluster else None
    query_str = """
        SELECT
             Settings.Names "name",
             Settings.Values "value"
        {% if cluster -%}
        FROM clusterAllReplicas({{ cluster }}, system.query_log)
        {% else -%}
        FROM system.query_log
        {% endif -%}
        ARRAY JOIN Settings
        WHERE type != 1
          AND query_id = '{{ query_id }}'
        """
    return execute_query(ctx, query_str, query_id=query_id, cluster=cluster)


def get_query_metrics(ctx, query_id, on_cluster=False):
    cluster = get_cluster_name(ctx) if on_cluster else None
    query_str = """
        SELECT
             ProfileEvents.Names "name",
             ProfileEvents.Values "value"
        {% if cluster -%}
        FROM clusterAllReplicas({{ cluster }}, system.query_log)
        {% else -%}
        FROM system.query_log
        {% endif -%}
        ARRAY JOIN ProfileEvents
        WHERE type != 1
          AND query_id = '{{ query_id }}'
        ORDER BY name
        """
    return execute_query(ctx, query_str, query_id=query_id, cluster=cluster)
