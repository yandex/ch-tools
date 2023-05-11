# -*- coding: utf-8 -*-

import click
import requests

from monrun_checks.clickhouse_client import ClickhouseClient
from common.result import Result


@click.command('geobase')
def geobase_command():
    """
    Check is ya.geobase present.
    """
    ch_client = ClickhouseClient()

    try:
        response = ch_client.execute('SELECT regionToName(CAST(1 AS UInt32))')[0][0]
        expected = u'Москва и Московская область'
        if response != expected:
            return Result(2, f'Geobase error, expected ({expected}), but got ({response})')
    except requests.exceptions.HTTPError as exc:
        return Result(2, repr(exc))

    return Result(0, 'OK')
