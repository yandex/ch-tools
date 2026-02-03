from typing import Any, Optional

from click import Context, group, option, pass_context

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.dictionary import (
    list_dictionaries,
    reload_dictionary,
)
from ch_tools.chadmin.internal.dictionary_migration import migrate_dictionaries
from ch_tools.common import logging
from ch_tools.common.cli.formatting import print_response


@group("dictionary", cls=Chadmin)
def dictionary_group() -> None:
    """Commands to manage external dictionaries."""
    pass


@dictionary_group.command("list")
@option("--name")
@option("--status")
@pass_context
def list_command(ctx: Context, name: str, status: str) -> None:
    """
    List dictionaries.
    """
    dictionaries = list_dictionaries(ctx, name=name, status=status)
    print_response(
        ctx,
        dictionaries,
        default_format="table",
    )


@dictionary_group.command("reload")
@option("--name")
@option("--status")
@pass_context
def reload_command(ctx: Context, name: str, status: str) -> None:
    """
    Reload one or several dictionaries.
    """
    dictionaries = list_dictionaries(ctx, name=name, status=status)
    for dictionary in dictionaries:
        logging.info("Reloading dictionary {}", _full_name(dictionary))
        reload_dictionary(ctx, database=dictionary["database"], name=dictionary["name"])


def _full_name(dictionary: Any) -> str:
    db_name = dictionary["database"]
    dict_name = dictionary["name"]

    if db_name:
        return f"`{db_name}`.`{dict_name}`"

    return f"`{dict_name}`"


@dictionary_group.command("migrate")
@option(
    "--dry-run",
    "dry_run",
    is_flag=True,
    default=False,
    help="Only print generated SQL statements without executing them.",
)
@option(
    "--remove",
    "should_remove",
    is_flag=True,
    default=False,
    help="Decide if old dictionaries should be removed.",
)
@option(
    "--force-reload",
    "force_reload",
    is_flag=True,
    default=False,
    help="Reload dictionaries after migration.",
)
@option(
    "--include",
    "include_pattern",
    default=None,
    type=str,
    help="Glob pattern to filter dictionaries by name.",
)
@option(
    "--exclude",
    "exclude_pattern",
    default=None,
    type=str,
    help="Glob pattern to exclude dictionaries by name.",
)
@option(
    "--database",
    "target_database",
    default=None,
    type=str,
    help="Target database for migrated dictionaries.",
)
@option(
    "--max-workers",
    "max_workers",
    default=4,
    type=int,
    help="Number of parallel workers.",
)
@pass_context
def migrate_command(
    ctx: Context,
    dry_run: bool,
    should_remove: bool,
    force_reload: bool,
    target_database: Optional[str],
    max_workers: int,
    include_pattern: Optional[str],
    exclude_pattern: Optional[str],
) -> None:
    """
    Migrate XML-dictionaries to DDL.
    """
    migrate_dictionaries(
        ctx,
        dry_run,
        should_remove,
        force_reload,
        target_database,
        max_workers,
        include_pattern,
        exclude_pattern,
    )
