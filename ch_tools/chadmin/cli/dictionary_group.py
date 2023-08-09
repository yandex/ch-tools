from click import group, option, pass_context

from ch_tools.chadmin.internal.dictionary import list_dictionaries, reload_dictionary
from ch_tools.common.cli.formatting import print_response


@group("dictionary")
def dictionary_group():
    """Commands to manage external dictionaries."""
    pass


@dictionary_group.command("list")
@option("--name")
@option("--status")
@pass_context
def list_command(ctx, name, status):
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
def reload_command(ctx, name, status):
    """
    Reload one or several dictionaries.
    """
    dictionaries = list_dictionaries(ctx, name=name, status=status)
    for dictionary in dictionaries:
        print(f"Reloading dictionary {_full_name(dictionary)}")
        reload_dictionary(ctx, database=dictionary["database"], name=dictionary["name"])


def _full_name(dictionary):
    db_name = dictionary["database"]
    dict_name = dictionary["name"]

    if db_name:
        return f"`{db_name}`.`{dict_name}`"

    return f"`{dict_name}`"
