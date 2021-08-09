from cli import execute_query
from click import command, pass_context


@command('stack-trace')
@pass_context
def stack_trace_command(ctx):
    """
    Collect stack traces.
    """
    query_str = r"""
        SELECT
            '\n' || arrayStringConcat(
               arrayMap(
                   x,
                   y -> concat(x, ': ', y),
                   arrayMap(x -> addressToLine(x), trace),
                   arrayMap(x -> demangle(addressToSymbol(x)), trace)),
               '\n') AS trace
        FROM system.stack_trace
    """
    print(execute_query(ctx, query_str, format='Vertical'))
