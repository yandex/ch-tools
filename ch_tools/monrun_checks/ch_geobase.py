# -*- coding: utf-8 -*-

import click
import requests

from ch_tools.common.clickhouse.client.clickhouse_client import clickhouse_client
from ch_tools.common.result import Result


@click.command("geobase")
@click.pass_context
def geobase_command(ctx):
    """
    Check that embedded geobase is configured.
    """

    try:
        response = clickhouse_client(ctx).query_json_data(
            query="SELECT regionToName(CAST(1 AS UInt32))"
        )[0][0]
        expected = "Москва и Московская область"
        if response != expected:
            return Result(
                2, f"Geobase error, expected ({expected}), but got ({response})"
            )
    except requests.exceptions.HTTPError as exc:
        return Result(2, repr(exc))

    return Result(0, "OK")
