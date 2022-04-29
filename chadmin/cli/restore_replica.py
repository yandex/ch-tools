from click import command, option, pass_context

from cloud.mdb.clickhouse.tools.chadmin.cli import execute_query, get_cluster_name
from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseError


@command('restore-replica')
@option('--cluster', '--on-cluster', 'on_cluster', is_flag=True, help='Run RESTORE REPLICA on cluster ')
@pass_context
def restore_replica_command(ctx, on_cluster):
    query = """
         SELECT database, name
         FROM system.tables
         WHERE database NOT IN ('system') AND engine LIKE '%Replicated%'
    """
    tables = execute_query(ctx, query, format='JSON')['data']
    query = """
         SELECT database, table
         FROM system.replicas
         WHERE is_readonly=1
    """
    ro_tables = execute_query(ctx, query, format='JSON')['data']
    ro_tables_list = []
    for ro_table in ro_tables:
        ro_tables_list.append(f"{ro_table['database']}.{ro_table['table']}")

    cluster = get_cluster_name(ctx) if on_cluster else None

    for table in tables:
        if f"{table['database']}.{table['name']}" in ro_tables_list:
            print("Database - {0}. Table - {1}".format(table['database'], table['name']))
            try:
                result = _restore(ctx, cluster, table['database'], table['name'])
            except ClickhouseError as e:
                msg = str(e)
                if 'Replica has metadata in ZooKeeper' in msg:
                    _restart(ctx, cluster, table['database'], table['name'])
                    result = _restore(ctx, cluster, table['database'], table['name'])
                elif 'Replica path is present' in msg:
                    result = _restart(ctx, cluster, table['database'], table['name'])
                else:
                    raise
            print(result)


def _restore(ctx, cluster, database, table):
    restore_query = """
        SYSTEM RESTORE REPLICA `{{ database }}`.`{{ table }}`
        {% if cluster %}
            ON CLUSTER '{{ cluster }}'
        {% endif %}
    """
    return execute_query(ctx, restore_query, database=database, table=table, cluster=cluster, format=None, timeout=600)


def _restart(ctx, cluster, database, table):
    restart_query = """
        SYSTEM RESTART REPLICA
        {% if cluster %}
            ON CLUSTER '{{ cluster }}'
        {% endif %}
            `{{ database }}`.`{{ table }}`
    """
    return execute_query(ctx, restart_query, database=database, table=table, cluster=cluster, format=None)
