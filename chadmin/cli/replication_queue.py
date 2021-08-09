from click import group, option, pass_context
from cloud.mdb.clickhouse.tools.chadmin.cli import execute_query


@group('replication-queue')
def replication_queue_group():
    pass


@replication_queue_group.command('list')
@option('--failed', is_flag=True)
@option('-v', '--verbose', is_flag=True)
@option('-l', '--limit')
@pass_context
def list_replication_queue_command(ctx, failed, verbose, limit):
    query = """
    SELECT
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
    FROM system.replication_queue
    {% if verbose %}
    JOIN system.replicas USING (database, table)
    {% endif %}
    WHERE 1
    {% if failed %}
      AND last_exception != ''
    {% endif %}
    ORDER BY table, position
    {% if limit is not none %}
    LIMIT {{ limit }}
    {% endif %}
    """
    print(execute_query(ctx,
                        query,
                        failed=failed,
                        verbose=verbose,
                        limit=limit,
                        format='Vertical'))
