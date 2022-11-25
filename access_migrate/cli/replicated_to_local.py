import os
import pwd
from typing import Optional

import click
from kazoo.client import KazooClient

CH_USER = 'clickhouse'
CH_ACCESS_PATH = '/var/lib/clickhouse/access'
CH_MARK_FILE = 'need_rebuild_lists.mark'

KEEPER_UUID_PATH = '/clickhouse/access/uuid'


@click.command('replicated')
@click.pass_context
def replicated_to_local(ctx) -> None:
    keeper: KazooClient = ctx.obj['keeper_cli']
    if keeper.exists(KEEPER_UUID_PATH) is None:
        click.echo('uuid node does not exist')
        return

    ch_user = _get_ch_user()
    if ch_user is None:
        click.echo('clickhouse user does not exist')
        return

    uuid_list = keeper.get_children(KEEPER_UUID_PATH)
    if not uuid_list:
        click.echo('uuid node is empty')
        return

    for uuid in uuid_list:
        data, _ = keeper.get(f'{KEEPER_UUID_PATH}/{uuid}')
        file_path = _create_file(f'{uuid}.sql', data.decode())
        _chown_file(file_path, ch_user)

    _mark_to_rebuild(ch_user)


def _create_file(file_name: str, file_content: str = '') -> str:
    file_path = os.path.join(CH_ACCESS_PATH, file_name)
    with open(file_path, 'w') as file:
        file.write(file_content)

    return file_path


def _chown_file(file_path: str, pwd_user: pwd.struct_passwd) -> None:
    if not os.path.exists(file_path):
        return
    os.chown(file_path, pwd_user.pw_uid, pwd_user.pw_gid)


def _get_ch_user() -> Optional[pwd.struct_passwd]:
    try:
        return pwd.getpwnam(CH_USER)
    except KeyError:
        return None


def _mark_to_rebuild(pwd_user: pwd.struct_passwd) -> None:
    path = _create_file(CH_MARK_FILE)
    _chown_file(path, pwd_user)
