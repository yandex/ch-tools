import json

import click

from ch_tools.chadmin.cli.object_storage_group import (
    ORPHANED_OBJECTS_ERROR_MSG_FIELD,
    ORPHANED_OBJECTS_SIZE_FIELD,
    STATE_LOCAL_PATH,
)
from ch_tools.chadmin.internal.zookeeper import get_zk_node
from ch_tools.common.result import CRIT, OK, WARNING, Result


@click.command("orphaned-objects")
@click.option(
    "--state-local",
    "state_local",
    is_flag=True,
    help="Get total size of orphaned objects from local file.",
)
@click.option(
    "--state-zk-path",
    "state_zk_path",
    help="Zookeeper node path from which the total size of orphaned objects will be taken.",
)
@click.option(
    "-c",
    "--critical",
    "crit",
    type=int,
    default=10 * 1024**3,
    help="Critical threshold.",
)
@click.option(
    "-w",
    "--warning",
    "warn",
    type=int,
    default=100 * 1024**2,
    help="Warning threshold.",
)
@click.pass_context
def orphaned_objects_command(
    ctx: click.Context,
    state_local: bool,
    state_zk_path: str,
    crit: int,
    warn: int,
) -> Result:
    _check_mutually_exclusive(state_local, state_zk_path)

    state = dict()
    if state_local:
        state = _local_get_orphaned_objects_state()

    if state_zk_path:
        state = _zk_get_orphaned_objects_state(ctx, state_zk_path)

    total_size = state.get(ORPHANED_OBJECTS_SIZE_FIELD)
    error_msg = state.get(ORPHANED_OBJECTS_ERROR_MSG_FIELD)

    msg = ""
    if total_size is None:
        msg += f'Orphaned objects state not have field "{ORPHANED_OBJECTS_SIZE_FIELD}".'
    if error_msg is None:
        msg += f'Orphaned objects state not have field "{ORPHANED_OBJECTS_ERROR_MSG_FIELD}".'

    if msg != "":
        return Result(CRIT, msg)

    if error_msg != "":
        return Result(CRIT, error_msg)

    msg = f"Total size: {total_size}"
    if isinstance(total_size, int) and total_size >= crit:
        return Result(CRIT, msg)
    if isinstance(total_size, int) and total_size >= warn:
        return Result(WARNING, msg)
    return Result(OK, msg)


def _check_mutually_exclusive(state_local, state_zk_path):
    if not state_local and not state_zk_path:
        raise click.UsageError(
            "One of these options must be provided: --state-local, --state-zk-path"
        )

    if state_local and state_zk_path:
        raise click.UsageError(
            "Options --state-local and --state-zk-path are mutually exclusive."
        )


def _local_get_orphaned_objects_state() -> dict:
    try:
        with open(STATE_LOCAL_PATH, mode="r", encoding="utf-8") as file:
            return json.load(file)
    except Exception as e:
        return {
            ORPHANED_OBJECTS_SIZE_FIELD: 0,
            ORPHANED_OBJECTS_ERROR_MSG_FIELD: str(e),
        }


def _zk_get_orphaned_objects_state(ctx: click.Context, state_zk_path: str) -> dict:
    try:
        return json.loads(get_zk_node(ctx, state_zk_path))
    except Exception as e:
        return {
            ORPHANED_OBJECTS_SIZE_FIELD: 0,
            ORPHANED_OBJECTS_ERROR_MSG_FIELD: str(e),
        }
