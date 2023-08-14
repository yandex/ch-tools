from collections import OrderedDict

from click import Choice, group, option, pass_context

from ch_tools.chadmin.internal.part import (
    attach_part,
    detach_part,
    drop_detached_part,
    drop_part,
    list_detached_parts,
    list_parts,
    move_part,
)
from ch_tools.chadmin.internal.system import match_ch_version
from ch_tools.common.cli.formatting import format_bytes, print_response
from ch_tools.common.cli.parameters import BytesParamType

FIELD_FORMATTERS = {
    "bytes_on_disk": format_bytes,
}


@group("part")
def part_group():
    """
    Commands to manage data parts.
    """
    pass


@part_group.command(name="list")
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
    ctx, active, min_size, max_size, detached, reason, order_by, **kwargs
):
    """List data parts."""

    def _table_formatter(part):
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


@part_group.command(name="attach")
@option(
    "-d", "--database", help="Filter in data parts to attach by the specified database."
)
@option("-t", "--table", help="Filter in data parts to attach by the specified table.")
@option(
    "--id",
    "--partition",
    "partition_id",
    help="Filter in data parts to attach by the specified partition.",
)
@option(
    "--name",
    "--part",
    "part_name",
    help="Filter in data parts to attach by the specified data part name.",
)
@option(
    "--disk", "disk_name", help="Filter in data parts to attach by the specified disk."
)
@option("-a", "--all", "all_", is_flag=True, help="Attach all data parts.")
@option("-l", "--limit", type=int, help="Limit the max number of data parts to attach.")
@option("-k", "--keep-going", is_flag=True, help="Do not stop on the first error.")
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def attach_parts_command(
    ctx,
    all_,
    database,
    table,
    partition_id,
    part_name,
    disk_name,
    limit,
    keep_going,
    dry_run,
):
    """Attach one or several parts."""
    if not any((all_, database, table, partition_id, part_name, disk_name, limit)):
        ctx.fail(
            "At least one of --all, --disk, --database, --table, --partition, --part, --limit"
            " options must be specified."
        )

    parts = list_detached_parts(
        ctx,
        database=database,
        table=table,
        partition_id=partition_id,
        part_name=part_name,
        disk_name=disk_name,
        reason="",
        limit=limit,
    )
    for part in parts:
        try:
            attach_part(
                ctx, part["database"], part["table"], part["name"], dry_run=dry_run
            )
        except Exception as e:
            if keep_going:
                print(repr(e))
            else:
                raise


@part_group.command(name="detach")
@option(
    "-d", "--database", help="Filter in data parts to detach by the specified database."
)
@option("-t", "--table", help="Filter in data parts to detach by the specified table.")
@option(
    "--id",
    "--partition",
    "partition_id",
    help="Filter in data parts to detach by the specified partition.",
)
@option(
    "--name",
    "--part",
    "part_name",
    help="Filter in data parts to detach by the specified data part name.",
)
@option(
    "--disk", "disk_name", help="Filter in data parts to detach by the specified disk."
)
@option("-a", "--all", "all_", is_flag=True, help="Detach all data parts.")
@option("-l", "--limit", type=int, help="Limit the max number of data parts to detach.")
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
    ctx,
    all_,
    database,
    table,
    partition_id,
    part_name,
    disk_name,
    limit,
    keep_going,
    dry_run,
):
    """Detach one or several parts."""
    if not any((all_, database, table, partition_id, part_name, disk_name, limit)):
        ctx.fail(
            "At least one of --all, --disk, --database, --table, --partition, --part, --limit"
            " options must be specified."
        )

    parts = list_parts(
        ctx,
        database=database,
        table=table,
        partition_id=partition_id,
        part_name=part_name,
        disk_name=disk_name,
        limit=limit,
    )
    for part in parts:
        try:
            detach_part(
                ctx, part["database"], part["table"], part["name"], dry_run=dry_run
            )
        except Exception as e:
            if keep_going:
                print(repr(e))
            else:
                raise


@part_group.command(name="delete")
@option(
    "-d", "--database", help="Filter in data parts to delete by the specified database."
)
@option("-t", "--table", help="Filter in data parts to delete by the specified table.")
@option(
    "--id",
    "--partition",
    "partition_id",
    help="Filter in data parts to delete by the specified partition.",
)
@option("--min-partition", "min_partition_id")
@option("--max-partition", "max_partition_id")
@option(
    "--name",
    "--part",
    "part_name",
    help="Filter in data parts to delete by the specified data part name.",
)
@option(
    "--disk", "disk_name", help="Filter in data parts to delete by the specified disk."
)
@option("--level", type=int)
@option("--min-level", type=int)
@option("--max-level", type=int)
@option("--min-size", type=BytesParamType())
@option("--max-size", type=BytesParamType())
@option("--detached", is_flag=True)
@option(
    "--reason", help="Filter in data parts to delete by the specified detach reason."
)
@option("-a", "--all", "all_", is_flag=True, help="Attach all data parts.")
@option("-l", "--limit", type=int, help="Limit the max number of data parts to delete.")
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
    ctx,
    database,
    table,
    partition_id,
    part_name,
    min_size,
    max_size,
    detached,
    reason,
    all_,
    limit,
    keep_going,
    dry_run,
    **kwargs,
):
    """Delete one or several data parts."""
    if not any((all_, database, table, partition_id, part_name, reason is not None)):
        ctx.fail(
            "At least one of --all, --database, --table, --partition, --part, --reason options must be specified."
        )

    if detached:
        parts = list_detached_parts(
            ctx,
            database=database,
            table=table,
            partition_id=partition_id,
            part_name=part_name,
            reason=reason,
            limit=limit,
            **kwargs,
        )
    else:
        if reason:
            ctx.fail("Option --reason cannot be used without --detached.")

        parts = list_parts(
            ctx,
            database=database,
            table=table,
            partition_id=partition_id,
            part_name=part_name,
            min_size=min_size,
            max_size=max_size,
            limit=limit,
            **kwargs,
        )
    for part in parts:
        try:
            if detached:
                drop_detached_part(
                    ctx, part["database"], part["table"], part["name"], dry_run=dry_run
                )
            else:
                drop_part(
                    ctx, part["database"], part["table"], part["name"], dry_run=dry_run
                )
        except Exception as e:
            if keep_going:
                print(repr(e))
            else:
                raise


@part_group.command(name="move")
@option(
    "-d", "--database", help="Filter in data parts to move by the specified database."
)
@option("-t", "--table", help="Filter in data parts to move by the specified table.")
@option(
    "--id",
    "--partition",
    "partition_id",
    help="Filter in data parts to move by the specified partition.",
)
@option("--min-partition", "min_partition_id")
@option("--max-partition", "max_partition_id")
@option(
    "--name",
    "--part",
    "part_name",
    help="Filter in data parts to move by the specified data part name.",
)
@option(
    "--disk", "disk_name", help="Filter in data parts to move by the specified disk."
)
@option("--new-disk", "new_disk_name", required=True)
@option("--min-size", type=BytesParamType())
@option("--max-size", type=BytesParamType())
@option("-l", "--limit", type=int, help="Limit the max number of data parts to move.")
@option("-k", "--keep-going", is_flag=True, help="Do not stop on the first error.")
@option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Enable dry run mode and do not perform any modifying actions.",
)
@pass_context
def move_parts_command(
    ctx,
    database,
    table,
    partition_id,
    part_name,
    disk_name,
    new_disk_name,
    limit,
    keep_going,
    dry_run,
    **kwargs,
):
    """Move one or several data parts."""
    if not any((database, table, partition_id, part_name, disk_name)):
        ctx.fail(
            "At least one of --database, --table, --partition, --part, --disk options must be specified."
        )

    parts = list_parts(
        ctx,
        database=database,
        table=table,
        partition_id=partition_id,
        part_name=part_name,
        disk_name=disk_name,
        active=True,
        limit=limit,
        **kwargs,
    )
    for part in parts:
        try:
            move_part(
                ctx,
                part["database"],
                part["table"],
                part["name"],
                new_disk_name,
                dry_run=dry_run,
            )
        except Exception as e:
            if keep_going:
                print(repr(e))
            else:
                raise
