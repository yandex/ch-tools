import os
import pwd
from kazoo.client import KazooClient
from typing import Optional

from click import echo, group, option, pass_context

from cloud.mdb.clickhouse.tools.chadmin.internal.zookeeper import zk_client

CH_USER = 'clickhouse'
CH_ACCESS_PATH = '/var/lib/clickhouse/access'
CH_MARK_FILE = 'need_rebuild_lists.mark'
KEEPER_UUID_PATH = '/clickhouse/access/uuid'


@group('access-migrate')
@option('--port', help='ZooKeeper port.', type=int, default=2181)
@option('--host', help='ZooKeeper host.', type=str, default='127.0.0.1')
@option(
    '--zkcli_identity',
    help='Identity for zookeeper cli shell. In a format login:password. '
    'Example: clickhouse:X7ui1dXIXXXXXXXXXXXXXXXXXXXXXXXX',
    type=str,
)
@pass_context
def access_migration_group(ctx, host: str, port: int, zkcli_identity: str) -> None:
    """Commands to migrate CH access entities."""
    ctx.obj['zk_client_args'] = {
        'port': port,
        'host': host,
        'zkcli_identity': zkcli_identity,
    }


@access_migration_group.command('replicated')
@pass_context
def replicated_to_local(ctx) -> None:
    if not os.path.exists(CH_ACCESS_PATH):
        echo('access folder does not exist')
        return

    with zk_client(ctx) as zk:
        access_files = os.listdir(CH_ACCESS_PATH)
        for file in access_files:
            uuid, file_ext = os.path.splitext(file)
            if file_ext != '.sql':
                continue
            file_data = _file_read(file)
            _upsert_zk_uuid(zk, uuid, file_data)


@access_migration_group.command('local')
@pass_context
def local_to_replicated(ctx) -> None:
    ch_user = _get_ch_user()
    if ch_user is None:
        echo('clickhouse user does not exist')
        return

    with zk_client(ctx) as zk:
        uuid_list = zk.get_children(KEEPER_UUID_PATH)
        if not uuid_list:
            echo('uuid node is empty')
            return

        for uuid in uuid_list:
            data, _ = zk.get(f'{KEEPER_UUID_PATH}/{uuid}')
            file_path = _file_create(f'{uuid}.sql', data.decode())
            _file_chown(file_path, ch_user)

        _mark_to_rebuild(ch_user)


def _upsert_zk_uuid(zk: KazooClient, uuid: str, data: str) -> None:
    value = data.encode()

    zk_path = f'{KEEPER_UUID_PATH}/{uuid}'
    if zk.exists(zk_path):
        zk.set(zk_path, value)
    else:
        zk.create(zk_path, value)


def _get_ch_user(user_name: str = CH_USER) -> Optional[pwd.struct_passwd]:
    try:
        return pwd.getpwnam(user_name)
    except KeyError:
        return None


def _file_create(file_name: str, file_content: str = '') -> str:
    file_path = os.path.join(CH_ACCESS_PATH, file_name)
    with open(file_path, 'w') as file:
        file.write(file_content)

    return file_path


def _file_read(file_name: str) -> str:
    file_path = os.path.join(CH_ACCESS_PATH, file_name)
    with open(file_path, 'r') as file:
        return file.read()


def _file_chown(file_path: str, pwd_user: pwd.struct_passwd) -> None:
    if not os.path.exists(file_path):
        return
    os.chown(file_path, pwd_user.pw_uid, pwd_user.pw_gid)


def _mark_to_rebuild(pwd_user: pwd.struct_passwd) -> None:
    path = _file_create(CH_MARK_FILE)
    _file_chown(path, pwd_user)
