from click import command, pass_context

from cloud.mdb.clickhouse.tools.chadmin.cli import execute_query


@command('restore-replica')
@option('--on_cluster', is_flag=True, help='Run RESTORE REPLICA on cluster ')
@pass_context
def restore_replica_command(ctx, on_cluster):
    query = """
         select database, name 
         from system.tables 
         where database not in ('system') and engine like '%Replicated%'
    """
    tables = execute_query(ctx, query, format='JSON')['data']
    for table in tables:
        print("Database - {0}. Table - {1}".format(table['database'], table['name']))
        restore_query = """
            SYSTEM RESTORE REPLICA {0}.`{1}` 
            {% if on_cluster %}
                ON CLUSTER '{{cluster}}'
            {% endif %}
        """.format(table['database'], table['name'])
        print(execute_query(ctx, restore_query, on_cluster=on_cluster, format=None))
