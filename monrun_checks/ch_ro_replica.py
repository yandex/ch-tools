import click

from cloud.mdb.clickhouse.tools.monrun_checks.clickhouse_client import ClickhouseClient
from cloud.mdb.clickhouse.tools.common.result import Result


@click.command('ro-replica')
def ro_replica_command():
    """
    Check for readonly replic tables.
    """
    ch_client = ClickhouseClient()

    response = ch_client.execute('SELECT database, table FROM system.replicas WHERE is_readonly')
    if response:
        return Result(2, f'Readonly replica tables: {response}')

    return Result(0, 'OK')
