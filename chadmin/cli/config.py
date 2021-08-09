from click import command, pass_context

from lib import print_response


@command('config')
@pass_context
def get_config_command(ctx):
    print_response(ctx, ctx.obj['chcli'].config())
