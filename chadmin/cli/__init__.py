import json

import sqlparse

from cloud.mdb.clickhouse.tools.common.clickhouse import ClickhouseConfig
from jinja2 import Environment
from kazoo.client import KazooClient


def execute_query(ctx, query, echo=None, dry_run=None, format=None, **kwargs):
    if format is None:
        format = 'PrettyCompact'

    rendered_query = _render_query(query, kwargs)

    if echo:
        print(sqlparse.format(rendered_query, reindent=True), '\n')

    if dry_run:
        return

    response = ctx.obj['chcli'].query(rendered_query, format=format)

    if format in ('JSON', 'JSONCompact', 'JSONEachRow'):
        return json.loads(response)

    return response


def zk_client(ctx, host, port, zkcli_identity):
    """
    Create and return KazooClient.
    """
    zk_config = get_config(ctx).zookeeper
    connect_str = ','.join(f'{host if host else node["host"]}:{port if port else node["port"]}' for node in zk_config['node'])
    if 'root' in zk_config:
        connect_str += zk_config['root']

    if zkcli_identity is None:
        if 'identity' in zk_config:
            zkcli_identity = zk_config['identity']

    auth_data = None
    if zkcli_identity is not None:
        auth_data=[("digest", zkcli_identity)]

    return KazooClient(connect_str, auth_data=auth_data)


def get_config(ctx):
    if 'config' not in ctx.obj:
        ctx.obj['config'] = ClickhouseConfig.load()

    return ctx.obj['config']


def get_macros(ctx):
    return get_config(ctx).macros


def get_cluster_name(ctx):
    return get_config(ctx).cluster_name


def _render_query(query, vars):
    env = Environment()

    env.globals['format_str_match'] = _format_str_match
    env.globals['format_str_imatch'] = _format_str_imatch

    template = env.from_string(query)
    return template.render(vars)


def _format_str_match(value):
    if value is None:
        return None

    if value.find(',') < 0:
        return "LIKE '{0}'".format(value)

    return "IN ({0})".format(','.join("'{0}'".format(item.strip()) for item in value.split(',')))


def _format_str_imatch(value):
    if value is None:
        return None

    return _format_str_match(value.lower())
