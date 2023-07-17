# -*- coding: utf-8 -*-

import click
import requests

from ch_tools.common.result import Result
from ch_tools.monrun_checks.clickhouse_client import ClickhouseClient


@click.command("geobase")
def geobase_command():
    """
    Check that embedded geobase is configured.
    """
    ch_client = ClickhouseClient()

    try:
        response = ch_client.execute("SELECT regionToName(CAST(1 AS UInt32))")[0][0]
        expected = "Москва и Московская область"
        if response != expected:
            return Result(
                2, f"Geobase error, expected ({expected}), but got ({response})"
            )
    except requests.exceptions.HTTPError as exc:
        return Result(2, repr(exc))

    return Result(0, "OK")
