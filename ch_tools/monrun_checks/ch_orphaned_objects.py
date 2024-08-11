import json

import click

from ch_tools.chadmin.cli.object_storage_group import (
    ORPHANED_OBJECTS_SIZE_FIELD,
    STATE_LOCAL_PATH,
)
from ch_tools.chadmin.internal.zookeeper import check_zk_node, get_zk_node
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

    total_size = 0
    if state_zk_path:
        total_size = _zk_get_total_size(ctx, state_zk_path)

    if state_local:
        total_size = _local_get_total_size()

    msg = f"Total size: {total_size}"
    if total_size >= crit:
        return Result(CRIT, msg)
    if total_size >= warn:
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


def _local_get_total_size() -> int:
    try:
        with open(STATE_LOCAL_PATH, mode="r", encoding="utf-8") as file:
            total_size = json.load(file).get(ORPHANED_OBJECTS_SIZE_FIELD)
    except FileNotFoundError:
        total_size = 0

    return total_size


def _zk_get_total_size(ctx: click.Context, state_zk_path: str) -> int:
    total_size = 0
    if check_zk_node(ctx, state_zk_path):
        total_size = json.loads(get_zk_node(ctx, state_zk_path)).get(
            ORPHANED_OBJECTS_SIZE_FIELD
        )
    return total_size
