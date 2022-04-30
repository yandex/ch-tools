from click import Choice, group, option, pass_context

from cloud.mdb.clickhouse.tools.chadmin.cli import execute_query


@group('part-log')
def part_log_group():
    pass


@part_log_group.command('list')
@option('--database')
@option('-t', '--table')
@option('--partition')
@option('--part')
@option('--date')
@option('--min-date')
@option('--max-date')
@option('--min-time')
@option('--max-time')
@option('--time')
@option('-v', '--verbose', is_flag=True)
@option('--order-by', type=Choice(['time', 'size', 'rows']), default='time')
@option('-l', '--limit', default=10)
@pass_context
def list_part_log_command(ctx, date, min_date, max_date, min_time, max_time, time, **kwargs):
    min_date = min_date or date
    max_date = max_date or date
    min_time = min_time or time
    max_time = max_time or time
    print(list_part_log(ctx, min_date=min_date, max_date=max_date, min_time=min_time, max_time=max_time, **kwargs))


def list_part_log(
    ctx,
    database=None,
    table=None,
    partition=None,
    part=None,
    min_date=None,
    max_date=None,
    min_time=None,
    max_time=None,
    order_by='time',
    limit=10,
    verbose=False,
):
    order_by = {
        'time': 'event_time',
        'size': 'size_in_bytes',
        'rows': 'rows',
    }.get(order_by)

    query_str = """
        SELECT
             event_type,
             event_time,
             duration_ms,
             database,
             table,
             part_name,
             partition_id,
             rows,
             formatReadableSize(size_in_bytes) "size",
             merged_from
        FROM system.part_log
        {% if database %}
        WHERE database {{ format_str_match(database) }}
        {% else %}
        WHERE database != 'system'
        {% endif %}
        {% if table %}
          AND table {{ format_str_match(table) }}
        {% endif %}
        {% if partition %}
          AND partition_id {{ format_str_match(partition) }}
        {% endif %}
        {% if part %}
          AND part_name {{ format_str_match(part) }}
        {% endif %}
        {% if min_date %}
          AND event_date >= toDate('{{ min_date }}')
        {% elif min_time %}
          AND event_date >= toDate('{{ min_time }}')
        {% endif %}
        {% if max_date %}
          AND event_date <= toDate('{{ max_date }}')
        {% elif max_time %}
          AND event_date <= toDate('{{ max_time }}')
        {% endif %}
        {% if not min_date and not max_date and not min_time and not max_time %}
          AND event_date = today()
        {% endif %}
        {% if min_time %}
          AND event_time - INTERVAL duration_ms/1000 second >= toDateTime('{{ min_time }}')
        {% endif %}
        {% if max_time %}
          AND event_time <= toDateTime('{{ max_time }}')
        {% endif %}
        ORDER BY {{ order_by }} DESC
        LIMIT {{ limit }}
        """
    return execute_query(
        ctx,
        query_str,
        database=database,
        table=table,
        partition=partition,
        part=part,
        min_date=min_date,
        max_date=max_date,
        min_time=min_time,
        max_time=max_time,
        order_by=order_by,
        limit=limit,
        verbose=verbose,
        format='Vertical',
    )
