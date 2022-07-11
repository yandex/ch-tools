from click import Choice, argument, group, option, pass_context

from cloud.mdb.clickhouse.tools.chadmin.cli import get_cluster_name
from cloud.mdb.clickhouse.tools.chadmin.internal.utils import execute_query


@group('process')
def process_group():
    """Process management commands."""
    pass


@process_group.command('get')
@argument('query_id')
@pass_context
def get_process_command(ctx, query_id):
    print(get_processes(ctx, query_id=query_id, verbose=True))


@process_group.command('list')
@option('-u', '--user')
@option('-U', '--exclude-user')
@option('--query')
@option('-v', '--verbose', is_flag=True)
@option('--cluster', '--on-cluster', 'on_cluster', is_flag=True, help='Get records from all hosts in the cluster.')
@option('--order-by', type=Choice(['elapsed', 'memory_usage']), default='elapsed')
@option('-l', '--limit')
@pass_context
def list_processes_command(ctx, user, exclude_user, query, verbose, on_cluster, order_by, limit):
    print(
        get_processes(
            ctx,
            user=user,
            exclude_user=exclude_user,
            query_pattern=query,
            on_cluster=on_cluster,
            limit=limit,
            order_by=order_by,
            verbose=verbose,
        )
    )


@process_group.command('kill')
@argument('query_id', required=False)
@option('-a', '--all', is_flag=True)
@option('-u', '--user')
@option('-U', '--exclude-user')
@pass_context
def kill_process_command(ctx, query_id, all, user, exclude_user):
    if not any((query_id, all, user, exclude_user)):
        ctx.fail('At least one of QUERY_ID, --all, --user and --exclude-user options must be specified.')

    query_str = """
        KILL QUERY
        WHERE 1
        {% if user %}
          AND user = '{{ user }}'
        {% endif %}
        {% if exclude_user %}
          AND user != '{{ exclude_user }}'
        {% endif %}
        {% if query_id %}
          AND query_id = '{{ query_id }}'
        {% endif %}
        """
    print(execute_query(ctx, query_str, query_id=query_id, user=user, exclude_user=exclude_user))


@process_group.command('get-settings')
@argument('query_id')
@pass_context
def get_process_settings_command(ctx, query_id):
    query_str = """
        SELECT
             Settings.Names "name",
             Settings.Values "value"
        FROM system.processes
        ARRAY JOIN Settings
        WHERE query_id = '{{ query_id }}'
        ORDER BY name
        """
    print(execute_query(ctx, query_str, query_id=query_id))


@process_group.command('get-metrics')
@argument('query_id')
@pass_context
def get_process_metrics_command(ctx, query_id):
    query_str = """
        SELECT
             ProfileEvents.Names "name",
             ProfileEvents.Values "value"
        FROM system.processes
        ARRAY JOIN ProfileEvents
        WHERE query_id = '{{ query_id }}'
        ORDER BY name
        """
    print(execute_query(ctx, query_str, query_id=query_id))


def get_processes(
    ctx,
    user=None,
    exclude_user=None,
    query_id=None,
    query_pattern=None,
    on_cluster=None,
    limit=None,
    order_by='elsapsed',
    verbose=False,
):
    cluster = get_cluster_name(ctx) if on_cluster else None
    query_str = """
        SELECT
        {% if cluster %}
             hostName() "host",
        {% endif %}
             query_id,
             elapsed,
             query,
             is_cancelled,
             concat(toString(read_rows), ' rows / ', formatReadableSize(read_bytes)) "read",
             concat(toString(written_rows), ' rows / ', formatReadableSize(written_bytes)) "written",
             formatReadableSize(memory_usage) "memory usage",
             user,
             multiIf(empty(client_name),
                     http_user_agent,
                     concat(client_name, ' ',
                            toString(client_version_major), '.',
                            toString(client_version_minor), '.',
        {% if not verbose %}
                            toString(client_version_patch))) "client"
        {% else %}
                            toString(client_version_patch))) "client",
             ProfileEvents.Names,
             ProfileEvents.Values,
             Settings.Names,
             Settings.Values
        {% endif %}
        {% if cluster %}
        FROM clusterAllReplicas({{ cluster }}, system.processes)
        {% else %}
        FROM system.processes
        {% endif %}
        WHERE 1
        {% if user %}
          AND user = '{{ user }}'
        {% endif %}
        {% if exclude_user %}
          AND user != '{{ exclude_user }}'
        {% endif %}
        {% if query_id %}
          AND query_id = '{{ query_id }}'
        {% endif %}
        {% if query_pattern %}
          AND lower(query) LIKE lower('{{ query_pattern }}')
        {% endif %}
        {% if not query_id %}
        ORDER BY {{ order_by }} DESC
        {% endif %}
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
        cluster=cluster,
        limit=limit,
        verbose=verbose,
        order_by=order_by,
        format='Vertical',
    )
