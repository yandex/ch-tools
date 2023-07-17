from click import command, pass_context

from ch_tools.chadmin.internal.utils import execute_query


@command("stack-trace")
@pass_context
def stack_trace_command(ctx):
    """
    Collect stack traces.
    """
    query_str = r"""
        SELECT
            thread_name,
            min(thread_id) AS min_thread_id,
            count() AS threads,
            '\n' || arrayStringConcat(
               arrayMap(
                   x,
                   y -> concat(x, ': ', y),
                   arrayMap(x -> addressToLine(x), trace),
                   arrayMap(x -> demangle(addressToSymbol(x)), trace)),
               '\n') AS trace
        FROM system.stack_trace
        GROUP BY thread_name, trace
        ORDER BY min_thread_id
    """
    print(execute_query(ctx, query_str, format_="Vertical"))
