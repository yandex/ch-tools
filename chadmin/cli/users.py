from click import group, pass_context

from lib import print_yaml


@group('user')
def user_group():
    pass


@user_group.command('config')
@pass_context
def get_config_command(ctx):
    print_yaml(ctx.obj['chcli'].users_config())
