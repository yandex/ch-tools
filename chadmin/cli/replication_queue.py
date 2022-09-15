from collections import defaultdict

from click import Choice, group, option, pass_context
from cloud.mdb.clickhouse.tools.chadmin.cli import get_cluster_name
from cloud.mdb.clickhouse.tools.chadmin.internal.utils import execute_query
from cloud.mdb.clickhouse.tools.chadmin.internal.zookeeper import delete_zk_node


@group('replication-queue')
def replication_queue_group():
    pass


@replication_queue_group.command('list')
@option('--cluster', '--on-cluster', 'on_cluster', is_flag=True, help='Get records from all hosts in the cluster.')
@option('--failed', is_flag=True, help='Output only failed replication queue tasks (tasks with non-empty exception).')
@option('--executing', is_flag=True, help='Output only executing replication queue tasks.')
@option(
    '--type',
    type=Choice(['GET_PART', 'MERGE_PARTS', 'MUTATE_PART']),
    help='Filter replication queue tasks to output by the specified type.',
)
@option('--database', help='Filter replication queue tasks to output by the specified database.')
@option('--table', help='Filter replication queue tasks to output by the specified table.')
@option('-v', '--verbose', is_flag=True)
@option('-l', '--limit', help='Limit the max number of objects in the output.')
@pass_context
def list_replication_queue_command(ctx, **kwargs):
    print(get_replication_queue_tasks(ctx, **kwargs, format='Vertical'))


@replication_queue_group.command('delete')
@option(
    '--type',
    type=Choice(['GET_PART', 'MERGE_PARTS', 'MUTATE_PART']),
    help='Filter replication queue tasks to delete by the specified type.',
)
@option('--failed', is_flag=True)
@option('--database', help='Filter replication queue tasks to delete by the specified database.')
@option('--table', help='Filter replication queue tasks to delete by the specified table.')
@pass_context
def delete_command(ctx, **kwargs):
    tasks = get_replication_queue_tasks(ctx, **kwargs, verbose=True, format='JSON')['data']
    for table, tasks in group_tasks_by_table(tasks).items():
        database, table = table

        print(f'Detaching table `{database}`.`{table}`')
        execute_query(ctx, f"""DETACH TABLE `{database}`.`{table}`""", timeout=300, echo=True, format=None)

        for task in tasks:
            zk_path = task['zk_path']
            print(f'Deleting task from ZooKeeper: {zk_path}')
            delete_zk_node(ctx, zk_path)

        print(f'Attaching table `{database}`.`{table}`')
        execute_query(ctx, f"""ATTACH TABLE `{database}`.`{table}`""", timeout=300, echo=True, format=None)


def get_replication_queue_tasks(
    ctx,
    *,
    on_cluster=None,
    failed=None,
    executing=None,
    type=None,
    database=None,
    table=None,
    verbose=None,
    limit=None,
    format=None,
):
    cluster = get_cluster_name(ctx) if on_cluster else None
    query = """
    SELECT
    {% if cluster %}
        hostName() "host",
    {% endif %}
        database,
        table,
        position,
        node_name,
    {% if verbose %}
        replica_path || '/queue/' || node_name "zk_path",
    {% endif %}
        type,
        is_currently_executing,
        source_replica,
        parts_to_merge,
        new_part_name,
        create_time,
        last_attempt_time attempt_time,
        last_exception exception,
        concat('time: ', toString(last_postpone_time), ', number: ', toString(num_postponed), ', reason: ', postpone_reason) postpone
    {% if cluster %}
    FROM clusterAllReplicas({{ cluster }}, system.replication_queue)
    {% else %}
    FROM system.replication_queue
    {% endif %}
    {% if verbose %}
    JOIN system.replicas USING (database, table)
    {% endif %}
    WHERE 1
    {% if database %}
      AND database = '{{ database }}'
    {% endif %}
    {% if table %}
      AND table = '{{ table }}'
    {% endif %}
    {% if failed %}
      AND last_exception != ''
    {% endif %}
    {% if executing %}
      AND is_currently_executing
    {% endif %}
    {% if type %}
      AND type = '{{ type }}'
    {% endif %}
    ORDER BY table, position
    {% if limit is not none %}
    LIMIT {{ limit }}
    {% endif %}
    """
    return execute_query(
        ctx,
        query,
        cluster=cluster,
        database=database,
        table=table,
        failed=failed,
        executing=executing,
        type=type,
        verbose=verbose,
        limit=limit,
        format=format,
    )


def group_tasks_by_table(tasks):
    result = defaultdict(list)
    for task in tasks:
        result[(task['database'], task['table'])].append(task)
    return result
