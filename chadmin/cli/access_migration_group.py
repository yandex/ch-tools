import os
import pwd
from kazoo.client import KazooClient
from typing import BinaryIO, Dict, NamedTuple, Optional, Union

from click import echo, group, option, pass_context

from cloud.mdb.clickhouse.tools.chadmin.internal.zookeeper import zk_client

CH_USER = 'clickhouse'
CH_ACCESS_PATH = '/var/lib/clickhouse/access'
CH_MARK_FILE = 'need_rebuild_lists.mark'

ZK_ACCESS_PATH = '/clickhouse/access'
ZK_UUID_PATH = f'{ZK_ACCESS_PATH}/uuid'

UUID_LEN = 36


class AccessInfo(NamedTuple):
    unique_char: str
    file_name: str

    def get_name(self):
        return ''


ACCESS_ENTITIES = (
    AccessInfo('P', 'row_policies.list'),
    AccessInfo('Q', 'quotas.list'),
    AccessInfo('R', 'roles.list'),
    AccessInfo('S', 'settings_profiles.list'),
    AccessInfo('U', 'users.list'),
)


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
        _migrate_sql_files(zk)
        _migrate_list_files(zk)


def _migrate_sql_files(zk: KazooClient) -> None:
    access_files = os.listdir(CH_ACCESS_PATH)
    for file in access_files:
        uuid, file_ext = os.path.splitext(file)
        if file_ext != '.sql':
            continue
        file_data = _file_read(file)
        _zk_upsert_data(zk, f'{ZK_UUID_PATH}/{uuid}', file_data)


def _migrate_list_files(zk: KazooClient) -> None:
    for entity in ACCESS_ENTITIES:
        pairs = _read_list_file(entity.file_name)
        for uuid, name in pairs.items():
            _zk_upsert_data(zk, f'{ZK_ACCESS_PATH}/{entity.unique_char}/{name}', uuid)


def _read_list_file(file_name: str) -> Dict[str, str]:
    """Returns <UUID, name> mapping for all entities from particular `*.list` file."""
    result = {}
    file_path = os.path.join(CH_ACCESS_PATH, file_name)
    with open(file_path, 'rb') as file:
        pairs_num = _decode_next_uint(file)
        for _ in range(pairs_num):
            name_len = _decode_next_uint(file)
            name = file.read(name_len).decode()
            uuid = file.read(UUID_LEN).decode()
            result[uuid] = name

    return result


def _decode_next_uint(buffer: BinaryIO) -> int:
    """Decode the next unsigned LEB128 encoded integer from a buffer.
    See: https://en.wikipedia.org/wiki/LEB128
    """
    res = 0
    for i in range(9):
        cur = ord(buffer.read(1))
        res = res + ((cur & 0x7F) << (i * 7))
        if (cur & 0x80) == 0:
            break

    return res


@access_migration_group.command('local')
@pass_context
def local_to_replicated(ctx) -> None:
    ch_user = _get_ch_user()
    if ch_user is None:
        echo('clickhouse user does not exist')
        return

    with zk_client(ctx) as zk:
        uuid_list = zk.get_children(ZK_UUID_PATH)
        if not uuid_list:
            echo('uuid node is empty')
            return

        for uuid in uuid_list:
            data, _ = zk.get(f'{ZK_UUID_PATH}/{uuid}')
            file_path = _file_create(f'{uuid}.sql', data.decode())
            _file_chown(file_path, ch_user)

        _mark_to_rebuild(ch_user)


def _zk_upsert_data(zk: KazooClient, path: str, value: Union[str, bytes]) -> None:
    if isinstance(value, str):
        value = value.encode()

    if zk.exists(path):
        zk.set(path, value)
    else:
        zk.create(path, value)


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
