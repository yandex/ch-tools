from click import command, option, pass_context

from cloud.mdb.clickhouse.tools.chadmin.cli import get_cluster_name
from cloud.mdb.clickhouse.tools.chadmin.internal.table import list_tables
from cloud.mdb.clickhouse.tools.chadmin.internal.table_replica import restart_table_replica, restore_table_replica
from cloud.mdb.clickhouse.tools.chadmin.internal.utils import execute_query
from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseError


@command('restore-replica')
@option('--cluster', '--on-cluster', 'on_cluster', is_flag=True, help='Run RESTORE REPLICA on cluster ')
@pass_context
def restore_replica_command(ctx, on_cluster):
    tables = list_tables(ctx, engine='%Replicated%')
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
            try:
                restore_table_replica(ctx, table['database'], table['name'], cluster=cluster)
            except ClickhouseError as e:
                msg = str(e)
                if 'Replica has metadata in ZooKeeper' in msg:
                    restart_table_replica(ctx, table['database'], table['name'], cluster=cluster)
                    restore_table_replica(ctx, table['database'], table['name'], cluster=cluster)
                elif 'Replica path is present' in msg:
                    restart_table_replica(ctx, table['database'], table['name'], cluster=cluster)
                else:
                    raise
