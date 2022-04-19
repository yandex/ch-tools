from click import command, option, pass_context

from cloud.mdb.clickhouse.tools.chadmin.cli import execute_query, get_cluster_name


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
    for table in tables:
        print("Database - {0}. Table - {1}".format(table['database'], table['name']))
        cluster = get_cluster_name(ctx) if on_cluster else None
        restore_query = """
            SYSTEM RESTORE REPLICA `{{ database }}`.`{{ table }}`
            {% if on_cluster %}
                ON CLUSTER '{{ cluster }}'
            {% endif %}
        """
        print(execute_query(ctx, restore_query,
              database=table['database'], table=table['name'], on_cluster=on_cluster, cluster=cluster, format=None))
