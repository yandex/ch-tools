from collections import OrderedDict
from typing import Any, Dict, List, Optional

from click import Context
from cloup import Choice, group, option, option_group, pass_context
from cloup.constraints import If, IsSet, RequireAtLeast, RequireExactly

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.part import (
    attach_part,
    detach_part,
    drop_detached_part,
    drop_detached_part_from_disk,
    drop_part,
    get_disks,
    list_detached_parts,
    list_parts,
    move_part,
    part_has_suffix,
    remove_detached_part_prefix_on_disk,
)
from ch_tools.chadmin.internal.system import match_ch_version
from ch_tools.common import logging
from ch_tools.common.cli.formatting import format_bytes, print_response
from ch_tools.common.cli.parameters import BytesParamType
from ch_tools.common.process_pool import WorkerTask, execute_tasks_in_parallel

FIELD_FORMATTERS = {
    "bytes_on_disk": format_bytes,
}


@group("part", cls=Chadmin)
def part_group() -> None:
    """
    Commands to manage data parts.
    """
    pass


@part_group.command("list")
@option(
    "-d", "--database", help="Filter in data parts to output by the specified database."
)
@option("-t", "--table", help="Filter in data parts to output by the specified table.")
@option(
    "--id",
    "--partition",
    "partition_id",
    help="Filter in data parts to output by the specified partition.",
)
@option("--min-partition", "min_partition_id")
@option("--max-partition", "max_partition_id")
@option(
    "--name",
    "--part",
    "part_name",
    help="Filter in data parts to output by the specified data part name.",
)
@option(
    "--disk", "disk_name", help="Filter in data parts to output by the specified disk."
)
@option(
    "--level", type=int, help="Filter in data parts to output by the specified level."
)
@option(
    "--min-level",
    type=int,
    help="Output data parts which level greater or equal to the specified value.",
)
@option(
    "--max-level",
    type=int,
    help="Output data parts which level less or equal to the specified value.",
)
@option(
    "--min-size",
    type=BytesParamType(),
    help="Output data parts which size greater or equal to the specified value.",
)
@option(
    "--max-size",
    type=BytesParamType(),
    help="Output data parts which size less or equal to the specified value.",
)
@option("--active", is_flag=True, help="Output only active data parts.")
@option("--detached", is_flag=True, help="Output detached parts instead of attached.")
@option(
    "--reason",
    help="Filter in data parts to output by reason. Applicable only for detached data parts.",
)
@option("--order-by", type=Choice(["size", "rows"]), help="Sorting order.")
@option(
    "-l", "--limit", type=int, help="Limit the max number of objects in the output."
)
@pass_context
def list_parts_command(
    ctx: Context,
    active: bool,
    min_size: Optional[int],
    max_size: Optional[int],
    detached: bool,
    reason: Optional[str],
    order_by: Optional[str],
    **kwargs: Any,
) -> None:
    """List data parts."""

    def _table_formatter(part: Dict[str, Any]) -> OrderedDict:
        result = OrderedDict()
        result["database"] = part["database"]
        result["table"] = part["table"]
        result["name"] = part["name"]
        if not detached:
            result["part_type"] = part["part_type"]
            result["active"] = part["active"]
        result["disk"] = part["disk_name"]
        if detached:
            result["reason"] = part["reason"]
            if match_ch_version(ctx, min_version="23.1"):
                result["size"] = part["bytes_on_disk"]
        else:
            result["min_time"] = part["min_time"]
            result["max_time"] = part["max_time"]
            result["rows"] = part["rows"]
            result["size"] = part["bytes_on_disk"]

        return result

    if detached:
        parts = list_detached_parts(ctx, reason=reason, **kwargs)
    else:
        parts = list_parts(
            ctx,
            active=active,
            min_size=min_size,
            max_size=max_size,
            order_by=order_by,
            **kwargs,
        )

    print_response(
        ctx,
        parts,
        default_format="table",
        table_formatter=_table_formatter,
        field_formatters=FIELD_FORMATTERS,
    )


@part_group.command("attach")
@option_group(
    "Part selection options",
    option(
        "-a",
        "--all",
        "_all",
        is_flag=True,
        help="Filter in all data parts.",
    ),
    option(
        "-d",
        "--database",
        help="Filter in data parts to attach by the specified database.",
    ),
    option(
        "-t", "--table", help="Filter in data parts to attach by the specified table."
    ),
    option(
        "--id",
        "--partition",
        "partition_id",
        help="Filter in data parts to attach by the specified partition.",
    ),
    option(
        "--name",
        "--part",
        "part_name",
        help="Filter in data parts to attach by the specified data part name.",
    ),
    option(
        "--disk",
        "disk_name",
        help="Filter in data parts to attach by the specified disk.",
    ),
    option(
        "-l",
        "--limit",
        type=int,
        help="Limit the max number of data parts to attach.",
    ),
    option(
        "--use-part-list-from-json",
        default=None,
        type=str,
        help="Use list of parts from the file. Example 'SELECT database, table, name ... FORMAT JSON' > file && chadmin part attach --use-part-list-from-json <file>.",
    ),
    constraint=If(
        IsSet("use_part_list_from_json"),
        then=RequireExactly(1),
        else_=RequireAtLeast(1),
    ),
)
@option("-k", "--keep-going", is_flag=True, help="Do not stop on the first error.")
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@option("-w", "--workers", default=4, help="Number of workers.")
@pass_context
def attach_parts_command(
    ctx: Context,
    _all: bool,
    keep_going: bool,
    dry_run: bool,
    workers: int,
    **kwargs: Any,
) -> None:
    """Attach one or several parts."""
    parts = list_detached_parts(ctx, reason="", **kwargs)

    tasks: List[WorkerTask] = []
    for part in parts:
        tasks.append(
            WorkerTask(
                f'{part["database"]}.{part["table"]}_{part["name"]}',
                attach_part,
                {
                    "ctx": ctx,
                    "database": part["database"],
                    "table": part["table"],
                    "part_name": part["name"],
                    "dry_run": dry_run,
                },
            )
        )
    execute_tasks_in_parallel(tasks, max_workers=workers, keep_going=keep_going)


@part_group.command("detach")
@option_group(
    "Part selection options",
    option(
        "-a",
        "--all",
        "_all",
        is_flag=True,
        help="Filter in all data parts.",
    ),
    option(
        "-d",
        "--database",
        help="Filter in data parts to detach by the specified database.",
    ),
    option(
        "-t",
        "--table",
        help="Filter in data parts to detach by the specified table.",
    ),
    option(
        "--id",
        "--partition",
        "partition_id",
        help="Filter in data parts to detach by the specified partition.",
    ),
    option(
        "--name",
        "--part",
        "part_name",
        help="Filter in data parts to detach by the specified data part name.",
    ),
    option(
        "--disk",
        "disk_name",
        help="Filter in data parts to detach by the specified disk.",
    ),
    option(
        "-l",
        "--limit",
        type=int,
        help="Limit the max number of data parts to detach.",
    ),
    option(
        "--use-part-list-from-json",
        default=None,
        type=str,
        help="Use list of parts from the file. Example 'SELECT database, table, name ... FORMAT JSON' > file && chadmin part detach --use-part-list-from-json <file>.",
    ),
    constraint=If(
        IsSet("use_part_list_from_json"),
        then=RequireExactly(1),
        else_=RequireAtLeast(1),
    ),
)
@option("-k", "--keep-going", is_flag=True, help="Do not stop on the first error.")
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def detach_parts_command(
    ctx: Context,
    _all: bool,
    keep_going: bool,
    dry_run: bool,
    **kwargs: Any,
) -> None:
    """Detach one or several parts."""
    parts = list_parts(ctx, **kwargs)
    for part in parts:
        try:
            detach_part(
                ctx,
                part["database"],
                part["table"],
                part["name"],
                dry_run=dry_run,
            )
        except Exception as e:
            if keep_going:
                logging.warning("{!r}\n", e)
            else:
                raise


@part_group.command("delete")
@option("--detached", is_flag=True)
@option_group(
    "Part selection options",
    option(
        "-a",
        "--all",
        "_all",
        is_flag=True,
        help="Filter in all data parts.",
    ),
    option(
        "-d",
        "--database",
        help="Filter in data parts to delete by the specified database.",
    ),
    option(
        "-t",
        "--table",
        help="Filter in data parts to delete by the specified table.",
    ),
    option(
        "--id",
        "--partition",
        "partition_id",
        help="Filter in data parts to delete by the specified partition.",
    ),
    option("--min-partition", "min_partition_id"),
    option("--max-partition", "max_partition_id"),
    option(
        "--name",
        "--part",
        "part_name",
        help="Filter in data parts to delete by the specified data part name.",
    ),
    option(
        "--disk",
        "disk_name",
        help="Filter in data parts to delete by the specified disk.",
    ),
    option("--level", type=int),
    option("--min-level", type=int),
    option("--max-level", type=int),
    option("--min-size", type=BytesParamType()),
    option("--max-size", type=BytesParamType()),
    option(
        "--reason",
        help="Filter in data parts to delete by the specified detach reason.",
    ),
    option(
        "-l",
        "--limit",
        type=int,
        help="Limit the max number of data parts to delete.",
    ),
    option(
        "--use-part-list-from-json",
        default=None,
        type=str,
        help="Use list of parts from the file. Example 'SELECT database, table, name ... FORMAT JSON' > file && chadmin part delete --use-part-list-from-json <file>.",
    ),
    constraint=If(
        IsSet("use_part_list_from_json"),
        then=RequireExactly(1),
        else_=RequireAtLeast(1),
    ),
)
@option("-k", "--keep-going", is_flag=True, help="Do not stop on the first error.")
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def delete_parts_command(
    ctx: Context,
    _all: bool,
    detached: bool,
    min_size: Optional[int],
    max_size: Optional[int],
    reason: Optional[str],
    keep_going: bool,
    dry_run: bool,
    **kwargs: Any,
) -> None:
    """Delete one or several data parts."""
    if detached:
        parts = list_detached_parts(
            ctx,
            reason=reason,
            **kwargs,
        )
    else:
        if reason:
            ctx.fail("Option --reason cannot be used without --detached.")

        parts = list_parts(
            ctx,
            min_size=min_size,
            max_size=max_size,
            **kwargs,
        )
    disks = get_disks(ctx)
    for part in parts:
        try:
            if detached:
                # ClickHouse can't parse detached parts with _tryN suffix
                # Should delete them using clickhouse-disks
                # TODO: Remove it after suffixes are allowed in ClickHouse
                if part_has_suffix(part["name"]):
                    drop_detached_part_from_disk(
                        ctx,
                        disks[part["disk_name"]],
                        part["path"],
                        dry_run,
                    )
                else:
                    drop_detached_part(
                        ctx,
                        part["database"],
                        part["table"],
                        part["name"],
                        dry_run=dry_run,
                    )
            else:
                drop_part(
                    ctx, part["database"], part["table"], part["name"], dry_run=dry_run
                )
        except Exception as e:
            if keep_going:
                logging.warning("{!r}\n", e)
            else:
                raise


@part_group.command("move")
@option_group(
    "Part selection options",
    option(
        "-d",
        "--database",
        help="Filter in data parts to move by the specified database.",
    ),
    option(
        "-t",
        "--table",
        help="Filter in data parts to move by the specified table.",
    ),
    option(
        "--id",
        "--partition",
        "partition_id",
        help="Filter in data parts to move by the specified partition.",
    ),
    option("--min-partition", "min_partition_id"),
    option("--max-partition", "max_partition_id"),
    option(
        "--name",
        "--part",
        "part_name",
        help="Filter in data parts to move by the specified data part name.",
    ),
    option(
        "--disk",
        "disk_name",
        help="Filter in data parts to move by the specified disk.",
    ),
    option("--min-size", type=BytesParamType()),
    option("--max-size", type=BytesParamType()),
    option(
        "-l",
        "--limit",
        type=int,
        help="Limit the max number of data parts to move.",
    ),
    option(
        "--use-part-list-from-json",
        default=None,
        type=str,
        help="Use list of parts from the file. Example 'SELECT database, table, name ... FORMAT JSON' > file && chadmin part delete --use-part-list-from-json <file>.",
    ),
    constraint=If(
        IsSet("use_part_list_from_json"),
        then=RequireExactly(1),
        else_=RequireAtLeast(1),
    ),
)
@option("--new-disk", "new_disk_name", required=True)
@option("-k", "--keep-going", is_flag=True, help="Do not stop on the first error.")
@option(
    "--sync/--async",
    "sync_mode",
    default=True,
    help="Enable/Disable synchronous query execution.",
)
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def move_parts_command(
    ctx: Context,
    new_disk_name: str,
    keep_going: bool,
    sync_mode: bool,
    dry_run: bool,
    **kwargs: Any,
) -> None:
    """Move one or several data parts."""
    parts = list_parts(ctx, active=True, **kwargs)
    for part in parts:
        try:
            move_part(
                ctx,
                part["database"],
                part["table"],
                part["name"],
                new_disk_name,
                sync_mode=sync_mode,
                dry_run=dry_run,
            )
        except Exception as e:
            if keep_going:
                logging.warning("{!r}\n", e)
            else:
                raise


@part_group.command("remove-detached-part-prefix")
@option_group(
    "Part selection options",
    option(
        "-a",
        "--all",
        "_all",
        is_flag=True,
        help="Filter in all data parts.",
    ),
    option(
        "-d",
        "--database",
        help="Filter in data parts to remove the prefix by the specified database.",
    ),
    option(
        "-t",
        "--table",
        help="Filter in data parts to remove the prefix by the specified table.",
    ),
    option(
        "--id",
        "--partition",
        "partition_id",
        help="Filter in data parts to remove the prefix by the specified partition.",
    ),
    option(
        "--name",
        "--part",
        "part_name",
        help="Filter in data parts to remove the prefix by the specified data part name.",
    ),
    option(
        "--disk",
        "disk_name",
        help="Filter in data parts to remove the prefix by the specified disk.",
    ),
    option(
        "-l",
        "--limit",
        type=int,
        help="Limit the max number of data parts to remove the prefix.",
    ),
    option(
        "--use-part-list-from-json",
        default=None,
        type=str,
        help="Use list of parts from the file. Example 'SELECT database, table, name ... FORMAT JSON' > file && chadmin part remove-detached-part-prefix --reason <> --use-part-list-from-json <file>.",
    ),
    constraint=If(
        IsSet("use_part_list_from_json"),
        then=RequireExactly(1),
        else_=RequireAtLeast(1),
    ),
)
@option("-k", "--keep-going", is_flag=True, help="Do not stop on the first error.")
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@option(
    "--reason",
    default="broken-on-start",
    type=str,
    help="Part reason of detach.",
)
@pass_context
def remove_detached_part_prefix_command(
    ctx: Context,
    keep_going: bool,
    dry_run: bool,
    _all: bool,
    reason: str,
    **kwargs: Any,
) -> None:
    """Move one or several data parts."""
    parts = list_detached_parts(
        ctx,
        reason=reason,
        use_part_list_required_columns_list=[
            "path",
        ],
        **kwargs,
    )
    for part in parts:
        try:
            remove_detached_part_prefix_on_disk(part["path"], reason + "_", dry_run)
        except Exception as e:
            if keep_going:
                logging.warning("{!r}\n", e)
            else:
                raise
