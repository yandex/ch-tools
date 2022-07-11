from click import command, option, pass_context

from cloud.mdb.clickhouse.tools.chadmin.internal.utils import execute_query


@command('dictionaries')
@option('-v', '--verbose', is_flag=True)
@pass_context
def list_dictionaries_command(ctx, verbose):
    if not verbose:
        fields = 'name, status, type, source'
        format = 'PrettyCompact'
    else:
        fields = '*'
        format = 'Vertical'

    print(execute_query(ctx, "SELECT {0} FROM system.dictionaries".format(fields), format=format))
