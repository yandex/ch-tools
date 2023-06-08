from click import command, option, pass_context

from chtools.chadmin.cli import get_cluster_name
from chtools.chadmin.internal.table_replica import restart_table_replica, restore_table_replica
from chtools.chadmin.internal.utils import execute_query
from chtools.common.clickhouse.client import ClickhouseError


@command('restore-replica')
@option('-d', '--database')
@option('-t', '--table')
@option('--cluster', '--on-cluster', 'on_cluster', is_flag=True, help='Run RESTORE REPLICA on cluster ')
@pass_context
def restore_replica_command(ctx, database, table, on_cluster):
    query = """
        SELECT database, table
        FROM system.replicas
        WHERE is_readonly = 1
        {% if database %}
          AND database {{ format_str_match(database) }}
        {% endif %}
        {% if table %}
          AND table {{ format_str_match(table) }}
        {% endif %}
        """
    ro_replicas = execute_query(ctx, query, database=database, table=table, format_='JSON')['data']

    cluster = get_cluster_name(ctx) if on_cluster else None

    for replica in ro_replicas:
        try:
            restore_table_replica(ctx, replica['database'], replica['table'], cluster=cluster)
        except ClickhouseError as e:
            msg = str(e)
            if 'Replica has metadata in ZooKeeper' in msg:
                restart_table_replica(ctx, replica['database'], replica['table'], cluster=cluster)
                restore_table_replica(ctx, replica['database'], replica['table'], cluster=cluster)
            elif 'Replica path is present' in msg:
                restart_table_replica(ctx, replica['database'], replica['table'], cluster=cluster)
            else:
                raise
