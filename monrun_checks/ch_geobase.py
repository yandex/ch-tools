# -*- coding: utf-8 -*-

import click

from cloud.mdb.clickhouse.tools.monrun_checks.clickhouse_client import ClickhouseClient
from cloud.mdb.clickhouse.tools.monrun_checks.result import Result


@click.command('geobase')
def geobase_command():
    """
    Check is ya.geobase present.
    """
    ch_client = ClickhouseClient()

    response = ch_client.execute('SELECT regionToName(CAST(1 AS UInt32))')[0][0]
    expected = u'Москва и Московская область'
    if response != expected:
        return Result(2, f'Geobase error, expected ({expected}), but got ({response})')

    return Result(0, 'OK')
