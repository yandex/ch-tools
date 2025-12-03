from typing import Any

from click import Context, group, option, pass_context

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.dictionary import (
    list_dictionaries,
    migrate_dictionaries,
    reload_dictionary,
)
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
@pass_context
def migrate_command(ctx: Context) -> None:
    """
    Migrate XML-dictionaries to DLL.
    """
    migrate_dictionaries(ctx)
