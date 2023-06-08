from click import group, option, pass_context
from chtools.common.cli.formatting import print_response
from chtools.chadmin.internal.dictionary import list_dictionaries, reload_dictionary


@group('dictionary')
def dictionary_group():
    """Commands to manage external dictionaries."""
    pass


@dictionary_group.command('list')
@option('--name')
@option('--status')
@pass_context
def list_command(ctx, name, status):
    """
    List dictionaries.
    """
    dictionaries = list_dictionaries(ctx, name=name, status=status)
    print_response(
        ctx,
        dictionaries,
        default_format='table',
    )


@dictionary_group.command('reload')
@option('--name')
@option('--status')
@pass_context
def reload_command(ctx, name, status):
    """
    Reload one or several dictionaries.
    """
    dictionaries = list_dictionaries(ctx, name=name, status=status)
    for dictionary in dictionaries:
        print(f'Reloading dictionary {_full_name(dictionary)}')
        reload_dictionary(ctx, database=dictionary['database'], name=dictionary['name'])


def _full_name(dictionary):
    database = dictionary['database']
    name = dictionary['name']
    if database:
        return f'`{database}`.`{name}`'
    else:
        return f'`{name}`'
