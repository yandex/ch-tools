import json

import click

from ch_tools.chadmin.cli.object_storage_group import (
    ORPHANED_OBJECTS_SIZE_FIELD,
    STORE_STATE_LOCAL_PATH,
)
from ch_tools.chadmin.internal.zookeeper import check_zk_node, get_zk_node
from ch_tools.common.result import CRIT, OK, WARNING, Result


@click.command("orphaned-objects")
@click.option(
    "--store-state-local",
    "store_state_local",
    is_flag=True,
    help="Get total size of orphaned objects from local file.",
)
@click.option(
    "--store-state-zk-path",
    "store_state_zk_path",
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
    store_state_local: bool,
    store_state_zk_path: str,
    crit: int,
    warn: int,
) -> Result:
    _check_mutually_exclusive(store_state_local, store_state_zk_path)

    total_size = 0
    if store_state_zk_path:
        total_size = _zk_get_total_size(ctx, store_state_zk_path)

    if store_state_local:
        total_size = _local_get_total_size()

    msg = f"Total size: {total_size}"
    if total_size >= crit:
        return Result(CRIT, msg)
    if total_size >= warn:
        return Result(WARNING, msg)
    return Result(OK, msg)


def _check_mutually_exclusive(store_state_local, store_state_zk_path):
    if not store_state_local and not store_state_zk_path:
        raise click.UsageError(
            "One of these options must be provided: --store_state_local, --store_state_zk_path"
        )

    if store_state_local and store_state_zk_path:
        raise click.UsageError(
            "Options --store-state-local and --store-state-zk-path are mutually exclusive."
        )


def _local_get_total_size() -> int:
    try:
        with open(STORE_STATE_LOCAL_PATH, mode="r", encoding="utf-8") as file:
            total_size = json.load(file).get(ORPHANED_OBJECTS_SIZE_FIELD)
    except FileNotFoundError:
        total_size = 0

    return total_size


def _zk_get_total_size(ctx: click.Context, store_state_zk_path: str) -> int:
    total_size = 0
    if check_zk_node(ctx, store_state_zk_path):
        total_size = json.loads(get_zk_node(ctx, store_state_zk_path)).get(
            ORPHANED_OBJECTS_SIZE_FIELD
        )
    return total_size
