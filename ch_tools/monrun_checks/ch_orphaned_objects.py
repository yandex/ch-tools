import re

import click

from ch_tools.chadmin.cli.object_storage_group import STATE_LOCAL_PATH
from ch_tools.chadmin.internal.object_storage.orphaned_objects_state import (
    OrphanedObjectsState,
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

    try:
        state = _get_orphaned_objects_state(ctx, state_local, state_zk_path)
    except Exception as e:
        return Result(CRIT, str(e))

    total_size = state.orphaned_objects_size
    error_msg = state.error_msg

    pattern = r"(Code:\s\d+\.\sDB::Exception:\s).*(\([A-Z_]*\)\s\(version\s.*\s\(official build\)\)).*"
    error_msg = re.sub(pattern, r"...", error_msg)

    if error_msg != "":
        return Result(CRIT, error_msg)

    result_msg = f"Total size: {total_size}"
    if total_size >= crit:
        return Result(CRIT, result_msg)
    if total_size >= warn:
        return Result(WARNING, result_msg)

    return Result(OK, result_msg)


def _check_mutually_exclusive(state_local, state_zk_path):
    if not state_local and not state_zk_path:
        raise click.UsageError(
            "One of these options must be provided: --state-local, --state-zk-path"
        )

    if state_local and state_zk_path:
        raise click.UsageError(
            "Options --state-local and --state-zk-path are mutually exclusive."
        )


def _get_orphaned_objects_state(
    ctx: click.Context, state_local: bool, state_zk_path: str
) -> "OrphanedObjectsState":
    if state_local:
        return _local_get_orphaned_objects_state()

    if state_zk_path:
        return _zk_get_orphaned_objects_state(ctx, state_zk_path)

    raise FileNotFoundError()


def _local_get_orphaned_objects_state() -> "OrphanedObjectsState":
    with open(STATE_LOCAL_PATH, mode="r", encoding="utf-8") as file:
        return OrphanedObjectsState.from_json(file.read())


def _zk_get_orphaned_objects_state(
    ctx: click.Context, state_zk_path: str
) -> "OrphanedObjectsState":
    zk_data = get_zk_node(ctx, state_zk_path)
    return OrphanedObjectsState.from_json(zk_data)
