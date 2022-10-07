import logging
import os
import sys
import time

from click import command, option, pass_context

from cloud.mdb.clickhouse.tools.chadmin.internal.utils import execute_query
from cloud.mdb.clickhouse.tools.common.utils import execute

BASE_TIMEOUT = 600
PART_LOAD_SPEED = 24  # in data parts per second


@command('wait-started')
@option(
    '--timeout',
    type=int,
    help='Max amount of time to wait, in seconds. If not set, the timeout is determined dynamically'
    ' based on data part count.',
)
@option('-q', '--quiet', is_flag=True, default=False, help='Quiet mode.')
@pass_context
def wait_started_command(ctx, timeout, quiet):
    """Wait for ClickHouse server to start up."""
    if not quiet:
        logging.basicConfig(level='INFO', format='%(message)s')

    if not timeout:
        timeout = BASE_TIMEOUT + int(get_data_dir_count() / PART_LOAD_SPEED)

    deadline = time.time() + timeout

    ch_is_alive = False
    while time.time() < deadline:
        if is_clickhouse_alive():
            ch_is_alive = True
            break
        time.sleep(1)

    if ch_is_alive:
        warmup_system_users(ctx)
        sys.exit(0)

    logging.error('ClickHouse is dead')
    sys.exit(1)


def get_data_dir_count():
    """
    Return the number of directories with data. data parts in ClickHouse data dir.
    """
    output = execute('find -L /var/lib/clickhouse/ -mindepth 4 -maxdepth 6 -type d | wc -l')
    return int(output)


def is_clickhouse_alive():
    """
    Check if ClickHouse server is alive or not.
    """
    try:
        os.chdir('/')
        output = execute('timeout 5 sudo -u monitor /usr/bin/ch-monitoring ping')
        if output == '0;OK\n':
            return True

    except Exception as e:
        logging.error('Failed to perform ch_ping check: %s', repr(e))

    return False


def warmup_system_users(ctx):
    execute_query(ctx, 'SELECT count() FROM system.users', timeout=300)
